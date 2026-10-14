// SPDX-License-Identifier: MIT

//! Active-chain metadata, roots, leaves and recovery-file references share one
//! atomic bucket. Recovery files are durable before their references are published.

use std::sync::RwLock;

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use bitcoin::block::Header;
use bitcoin::consensus::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, OutPoint};
use kv::{Batch, Store};
use serde::{Deserialize, Serialize};

use crate::journal::Journal;
use crate::udata::LeafContext;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tip {
    pub height: u32,
    pub block_hash: BlockHash,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeafChange {
    pub outpoint: OutPoint,
    pub before: Option<LeafContext>,
    pub after: Option<LeafContext>,
}

/// Enough to replay the accumulator and undo the leaf index, without blocks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta {
    pub tip: Tip,
    pub parent: BlockHash,
    pub adds: Vec<LeafContext>,
    pub dels: Vec<LeafContext>,
    pub changes: Vec<LeafChange>,
}

pub struct ChainView {
    data: kv::Bucket<'static, Vec<u8>, Vec<u8>>,
    journal: Journal,
    /// Serializes publication against multi-field reads by REST and P2P.
    pub(crate) gate: RwLock<()>,
}

fn key(prefix: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = vec![prefix];
    key.extend_from_slice(suffix);
    key
}

/// Sled retires pages using Crossbeam's default epoch collector. Its automatic
/// collection runs every 128 pins, not every write in an atomic batch. Large
/// batches can therefore enqueue garbage much faster than it is collected.
/// Flush after releasing Sled's batch guard, with work proportional to writes.
/// A fresh guard each time lets epochs advance; concurrently pinned readers
/// remain protected. The next batch can reclaim anything they still hold here.
fn reclaim_batch(writes: usize) {
    for _ in 0..writes.max(16) {
        crossbeam_epoch::pin().flush();
    }
}

impl ChainView {
    pub fn new(storage: Store) -> Result<Self> {
        Ok(Self {
            journal: Journal::new(storage.path()?.join("journal"))?,
            data: storage.bucket(Some("bridge-v2"))?,
            gate: RwLock::new(()),
        })
    }

    pub fn initialize(&self, header: Header) -> Result<()> {
        let hash = header.block_hash();
        if let Some(genesis) = self.data.get(&b"genesis".to_vec())? {
            ensure!(
                genesis == serialize(&hash),
                "backend network differs from stored genesis"
            );
            ensure!(self.tip()?.is_some(), "missing committed tip");
            self.migrate_journal()?;
            return self.clean_journal_orphans();
        }
        let mut batch = Batch::new();
        batch.set(&b"genesis".to_vec(), &serialize(&hash))?;
        Self::metadata(
            &mut batch,
            Tip {
                height: 0,
                block_hash: hash,
            },
            &header,
            &0u64.to_le_bytes(),
        )?;
        self.apply_batch(batch, 6)?;
        self.flush()?;
        self.clean_journal_orphans()
    }

    pub fn tip(&self) -> Result<Option<Tip>> {
        self.data
            .get(&b"tip".to_vec())?
            .map(|v| Ok(serde_json::from_slice(&v)?))
            .transpose()
    }

    fn metadata(
        batch: &mut Batch<Vec<u8>, Vec<u8>>,
        tip: Tip,
        header: &Header,
        roots: &[u8],
    ) -> Result<()> {
        ensure!(
            header.block_hash() == tip.block_hash,
            "header identity mismatch"
        );
        batch.set(&b"tip".to_vec(), &serde_json::to_vec(&tip)?)?;
        batch.set(
            &key(b'n', &tip.height.to_be_bytes()),
            &serialize(&tip.block_hash),
        )?;
        batch.set(
            &key(b'r', tip.block_hash.as_byte_array()),
            &tip.height.to_be_bytes().to_vec(),
        )?;
        batch.set(
            &key(b'h', tip.block_hash.as_byte_array()),
            &serialize(header),
        )?;
        batch.set(&key(b'a', tip.block_hash.as_byte_array()), &roots.to_vec())?;
        Ok(())
    }

    pub fn commit(&self, delta: &Delta, header: &Header, roots: &[u8]) -> Result<()> {
        let _guard = self
            .gate
            .write()
            .map_err(|_| anyhow::anyhow!("chain lock poisoned"))?;
        let previous = self.tip()?.context("missing committed tip")?;
        ensure!(
            delta.tip.height == previous.height + 1 && delta.parent == previous.block_hash,
            "block does not extend committed tip"
        );
        ensure!(header.prev_blockhash == delta.parent, "wrong block parent");
        // This is the write-ahead record: a crash before the DB batch leaves
        // an orphan file; a crash after it leaves a complete recoverable block.
        let reference = self.journal.write(delta)?;
        #[cfg(test)]
        crash_point("journal-durable");
        let mut batch = Batch::new();
        for change in &delta.changes {
            Self::write_leaf(&mut batch, change.outpoint, change.after.as_ref())?;
        }
        Self::metadata(&mut batch, delta.tip, header, roots)?;
        batch.set(&key(b'j', &delta.tip.height.to_be_bytes()), &reference)?;
        self.apply_batch(batch, delta.changes.len() + 6)?;
        Ok(())
    }

    fn apply_batch(&self, batch: Batch<Vec<u8>, Vec<u8>>, writes: usize) -> Result<()> {
        let result = self.data.batch(batch);
        reclaim_batch(writes);
        result?;
        Ok(())
    }

    fn write_leaf(
        batch: &mut Batch<Vec<u8>, Vec<u8>>,
        outpoint: OutPoint,
        leaf: Option<&LeafContext>,
    ) -> Result<()> {
        let key = key(b'l', &serialize(&outpoint));
        match leaf {
            Some(leaf) => batch.set(&key, &serialize(leaf))?,
            None => batch.remove(&key)?,
        }
        Ok(())
    }

    pub fn undo_tip(&self) -> Result<Tip> {
        let _guard = self
            .gate
            .write()
            .map_err(|_| anyhow::anyhow!("chain lock poisoned"))?;
        let tip = self.tip()?.context("missing tip")?;
        ensure!(tip.height > 0, "cannot disconnect genesis");
        let delta = self
            .delta(tip.height)?
            .context("reorg exceeds retained recovery journal; rebuild with a fresh DATA_DIR")?;
        ensure!(delta.tip == tip, "recovery journal identity mismatch");
        let previous = Tip {
            height: tip.height - 1,
            block_hash: delta.parent,
        };
        ensure!(
            self.get_block_hash(previous.height)? == Some(previous.block_hash),
            "missing reorg parent"
        );
        let mut batch = Batch::new();
        for change in &delta.changes {
            Self::write_leaf(&mut batch, change.outpoint, change.before.as_ref())?;
        }
        batch.set(&b"tip".to_vec(), &serde_json::to_vec(&previous)?)?;
        batch.remove(&key(b'n', &tip.height.to_be_bytes()))?;
        batch.remove(&key(b'j', &tip.height.to_be_bytes()))?;
        // Hash-keyed historical roots/headers remain available for diagnostics;
        // active_state rejects them once they leave the accepted chain.
        self.apply_batch(batch, delta.changes.len() + 3)?;
        // Never unlink an undo record while the durable tip might still need it.
        self.flush()?;
        #[cfg(test)]
        crash_point("undo-durable");
        self.journal.remove(tip)?;
        self.journal.sync()?;
        Ok(previous)
    }

    pub fn delta(&self, height: u32) -> Result<Option<Delta>> {
        self.data
            .get(&key(b'j', &height.to_be_bytes()))?
            .map(|v| self.journal.read(height, &v))
            .transpose()
    }

    pub fn prune_journal(&self, through: u32) -> Result<()> {
        loop {
            let mut batch = Batch::new();
            let mut removed = Vec::new();
            // Bounded metadata batches also bound Sled's deferred reclamation.
            for item in self.data.iter_prefix(&vec![b'j'])?.take(64) {
                let item = item?;
                let k: Vec<u8> = item.key()?;
                let height = u32::from_be_bytes(k[1..].try_into()?);
                if height > through {
                    break;
                }
                let reference: Vec<u8> = item.value()?;
                removed.push(Tip {
                    height,
                    block_hash: Journal::reference_hash(&reference)?,
                });
                batch.remove(&k)?;
            }
            if removed.is_empty() {
                break;
            }
            self.apply_batch(batch, removed.len())?;
            self.flush()?;
            #[cfg(test)]
            crash_point("prune-durable");
            for tip in removed {
                self.journal.remove(tip)?;
            }
            self.journal.sync()?;
        }
        Ok(())
    }

    /// Upgrade the existing bridge-v2 journal without a full resync or a bulk
    /// scan of blob values. Each old record is replaced only after its file is
    /// durable. Restart can resume from any mixture of old/new records.
    fn migrate_journal(&self) -> Result<()> {
        let mut cursor = vec![b'j'];
        let mut migrated = 0;
        loop {
            let Some(item) = self.data.next_key(&cursor)? else {
                break;
            };
            let k: Vec<u8> = item.key()?;
            if k.first() != Some(&b'j') {
                break;
            }
            let value: Vec<u8> = item.value()?;
            drop(item); // Do not pin the old value while replacing it.
            if !Journal::is_reference(&value) {
                let delta: Delta = serde_json::from_slice(&value)?;
                ensure!(
                    k == key(b'j', &delta.tip.height.to_be_bytes()),
                    "journal height mismatch"
                );
                ensure!(
                    self.get_block_hash(delta.tip.height)? == Some(delta.tip.block_hash),
                    "journal chain mismatch"
                );
                let reference = self.journal.write(&delta)?;
                #[cfg(test)]
                crash_point("migration-file-durable");
                drop(delta);
                drop(value);
                let mut batch = Batch::new();
                batch.set(&k, &reference)?;
                self.apply_batch(batch, 1)?;
                self.flush()?;
                #[cfg(test)]
                crash_point("migration-reference-durable");
                migrated += 1;
                if migrated % 100 == 0 {
                    log::info!("Migrated {migrated} recovery records to files");
                }
            } else {
                Journal::reference_hash(&value)?;
            }
            cursor = k;
        }
        if migrated != 0 {
            log::info!("Migrated {migrated} recovery records to files");
        }
        Ok(())
    }

    fn clean_journal_orphans(&self) -> Result<()> {
        self.journal.clean_orphans(|tip| {
            self.data
                .get(&key(b'j', &tip.height.to_be_bytes()))?
                .map(|reference| Ok(Journal::reference_hash(&reference)? == tip.block_hash))
                .unwrap_or(Ok(false))
        })
    }

    pub fn leaf(&self, outpoint: &OutPoint) -> Result<Option<LeafContext>> {
        self.data
            .get(&key(b'l', &serialize(outpoint)))?
            .map(|v| Ok(deserialize(&v)?))
            .transpose()
    }

    #[cfg(feature = "memory-leaf-map")]
    pub fn all_leaves(&self) -> Result<std::collections::HashMap<OutPoint, LeafContext>> {
        let mut leaves = std::collections::HashMap::new();
        for item in self.data.iter_prefix(&vec![b'l'])? {
            let item = item?;
            let k: Vec<u8> = item.key()?;
            let v: Vec<u8> = item.value()?;
            leaves.insert(deserialize(&k[1..])?, deserialize(&v)?);
        }
        Ok(leaves)
    }

    pub fn get_acc(&self, hash: BlockHash) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(&key(b'a', hash.as_byte_array()))?)
    }

    /// Caller holds `gate` while combining this with other chain metadata.
    #[cfg(not(feature = "shinigami"))]
    pub fn state(&self, hash: BlockHash) -> Result<Option<crate::state::State>> {
        let Some(tip) = self.active_tip_for(hash)? else {
            return Ok(None);
        };
        self.get_acc(hash)?
            .map(|bytes| crate::state::State::decode(tip, &bytes))
            .transpose()
    }

    pub fn flush(&self) -> Result<()> {
        self.data.flush()?;
        Ok(())
    }

    pub fn get_block(&self, hash: BlockHash) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(&key(b'h', hash.as_byte_array()))?)
    }

    /// Bitcoin's upper median of this block's time and up to ten ancestors.
    /// Follow hash-bound headers so a concurrent reorg cannot mix branches.
    pub fn median_time_past(&self, tip: Tip) -> Result<u32> {
        let mut hash = tip.block_hash;
        let mut times = Vec::with_capacity(11);
        for _ in 0..=tip.height.min(10) {
            let header: Header = deserialize(
                &self
                    .get_block(hash)?
                    .context("missing median-time header")?,
            )?;
            ensure!(header.block_hash() == hash, "median-time header mismatch");
            times.push(header.time);
            hash = header.prev_blockhash;
        }
        times.sort_unstable();
        Ok(times[times.len() / 2])
    }

    pub fn get_block_hash(&self, height: u32) -> Result<Option<BlockHash>> {
        self.data
            .get(&key(b'n', &height.to_be_bytes()))?
            .map(|v| Ok(deserialize(&v)?))
            .transpose()
    }

    pub fn get_height(&self, hash: BlockHash) -> Result<Option<u32>> {
        self.data
            .get(&key(b'r', hash.as_byte_array()))?
            .map(|v| Ok(u32::from_be_bytes(v.as_slice().try_into()?)))
            .transpose()
    }

    pub fn active_tip_for(&self, hash: BlockHash) -> Result<Option<Tip>> {
        let Some(height) = self.get_height(hash)? else {
            return Ok(None);
        };
        if self.get_block_hash(height)? != Some(hash) {
            return Ok(None);
        }
        Ok(Some(Tip {
            height,
            block_hash: hash,
        }))
    }
}

#[cfg(test)]
fn crash_point(phase: &str) {
    if std::env::var("BRIDGE_TEST_CRASH_PHASE").as_deref() == Ok(phase) {
        // No destructors or DB shutdown flush: exercise unclean process recovery.
        std::process::exit(73);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{block, outpoint, store, TestChain, TestDir};
    use bitcoin::{Block, ScriptBuf};

    fn rss_bytes() -> u64 {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
            * 1024
    }

    #[test]
    #[ignore = "standalone Sled batch reclamation regression; run optimized with --nocapture"]
    fn batch_memory() {
        let dir = TestDir::new();
        let db =
            kv::Store::new(kv::Config::new(dir.join("chain")).cache_capacity(1024 * 1024)).unwrap();
        let bucket = db.bucket::<Vec<u8>, Vec<u8>>(Some("batch-memory")).unwrap();
        let legacy = std::env::var_os("BRIDGE_BATCH_STRESS_LEGACY").is_some();
        let mut baseline = 0;
        for batch_number in 0u64..128 {
            let mut batch = Batch::new();
            for offset in 0..4096 {
                let index = batch_number * 4096 + offset;
                let key = bitcoin::hashes::sha256::Hash::hash(&index.to_le_bytes())
                    .to_byte_array()
                    .to_vec();
                batch.set(&key, &vec![42; 128]).unwrap();
            }
            bucket.batch(batch).unwrap();
            if !legacy {
                reclaim_batch(4096);
            }
            bucket.flush().unwrap();
            let count = batch_number + 1;
            if count == 32 {
                baseline = rss_bytes();
            }
            if count % 32 == 0 {
                println!(
                    "batch_memory legacy={legacy} batches={count} records={} rss_bytes={}",
                    count * 4096,
                    rss_bytes()
                );
            }
        }
        if !legacy {
            assert!(
                rss_bytes().saturating_sub(baseline) < 64 * 1024 * 1024,
                "retired Sled pages accumulated across batches"
            );
        }
    }

    fn fixture() -> (Block, Block, Delta) {
        let genesis = TestChain::new().0.read().unwrap()[0].clone();
        let first = block(&genesis, 1, vec![]);
        let op = outpoint(&first.txdata[0]);
        let leaf = LeafContext {
            block_hash: first.block_hash(),
            txid: op.txid,
            vout: op.vout,
            value: 50_000,
            pk_script: ScriptBuf::from_bytes(vec![0x51]),
            block_height: 1,
            median_time_past: genesis.header.time,
            is_coinbase: true,
        };
        let delta = Delta {
            tip: Tip {
                height: 1,
                block_hash: first.block_hash(),
            },
            parent: genesis.block_hash(),
            adds: vec![leaf.clone()],
            dels: vec![],
            changes: vec![LeafChange {
                outpoint: op,
                before: None,
                after: Some(leaf),
            }],
        };
        (genesis, first, delta)
    }

    #[test]
    fn recovery_crash_child() {
        let Ok(path) = std::env::var("BRIDGE_TEST_CRASH_PATH") else {
            return;
        };
        let phase = std::env::var("BRIDGE_TEST_CRASH_PHASE").unwrap();
        let view = ChainView::new(store(std::path::Path::new(&path))).unwrap();
        let (genesis, first, delta) = fixture();
        view.initialize(genesis.header).unwrap();
        match phase.as_str() {
            "journal-durable" => view.commit(&delta, &first.header, b"roots").unwrap(),
            "undo-durable" => {
                view.undo_tip().unwrap();
            }
            "prune-durable" => view.prune_journal(1).unwrap(),
            "commit-durable" => {
                view.commit(&delta, &first.header, b"roots").unwrap();
                view.flush().unwrap();
                crash_point("commit-durable");
            }
            _ => panic!("migration should have exited in initialize"),
        }
        panic!("crash point was not reached");
    }

    #[test]
    fn crash_boundaries_preserve_tip_leaves_and_recovery_records() {
        let (genesis, first, delta) = fixture();
        for phase in [
            "journal-durable",
            "commit-durable",
            "undo-durable",
            "prune-durable",
            "migration-file-durable",
            "migration-reference-durable",
        ] {
            let dir = TestDir::new();
            let path = dir.join("chain");
            let view = ChainView::new(store(&path)).unwrap();
            view.initialize(genesis.header).unwrap();
            if !matches!(phase, "journal-durable" | "commit-durable") {
                view.commit(&delta, &first.header, b"roots").unwrap();
                view.flush().unwrap();
            }
            if phase.starts_with("migration-") {
                // Exact old bridge-v2 representation, including its leaf/tip state.
                view.data
                    .set(
                        &key(b'j', &1u32.to_be_bytes()),
                        &serde_json::to_vec(&delta).unwrap(),
                    )
                    .unwrap();
                view.flush().unwrap();
                view.journal.remove(delta.tip).unwrap();
                view.journal.sync().unwrap();
            }
            drop(view);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "chainview::tests::recovery_crash_child",
                    "--nocapture",
                ])
                .env("BRIDGE_TEST_CRASH_PATH", &path)
                .env("BRIDGE_TEST_CRASH_PHASE", phase)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(73), "{phase}");
            let view = ChainView::new(store(&path)).unwrap();
            view.initialize(genesis.header).unwrap();
            let rolled_back = matches!(phase, "journal-durable" | "undo-durable");
            assert_eq!(
                view.tip().unwrap().unwrap().height,
                if rolled_back { 0 } else { 1 },
                "{phase}"
            );
            assert_eq!(
                view.leaf(&delta.changes[0].outpoint).unwrap(),
                if rolled_back {
                    None
                } else {
                    delta.changes[0].after.clone()
                },
                "{phase}"
            );
            let retained = !rolled_back && phase != "prune-durable";
            assert_eq!(view.delta(1).unwrap().is_some(), retained, "{phase}");
            assert_eq!(
                std::fs::read_dir(path.join("journal")).unwrap().count(),
                usize::from(retained),
                "{phase}"
            );
            if retained {
                assert_eq!(
                    serde_json::to_value(view.delta(1).unwrap().unwrap()).unwrap(),
                    serde_json::to_value(&delta).unwrap()
                );
                assert_eq!(
                    view.data
                        .get(&key(b'j', &1u32.to_be_bytes()))
                        .unwrap()
                        .unwrap()
                        .len(),
                    72
                );
            }
        }
    }

    #[test]
    fn corrupt_or_missing_committed_journal_fails_before_undo() {
        let (genesis, first, delta) = fixture();
        for missing in [false, true] {
            let dir = TestDir::new();
            let view = ChainView::new(store(&dir.join("chain"))).unwrap();
            view.initialize(genesis.header).unwrap();
            view.commit(&delta, &first.header, b"roots").unwrap();
            view.flush().unwrap();
            let path = std::fs::read_dir(dir.join("chain/journal"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            if missing {
                std::fs::remove_file(path).unwrap();
            } else {
                // Valid JSON with wrong contents must still fail its checksum.
                let mut corrupt = delta.clone();
                corrupt.adds[0].value += 1;
                std::fs::write(path, serde_json::to_vec(&corrupt).unwrap()).unwrap();
            }
            assert!(view.undo_tip().is_err());
            assert_eq!(view.tip().unwrap(), Some(delta.tip));
            assert_eq!(
                view.leaf(&delta.changes[0].outpoint).unwrap(),
                delta.changes[0].after
            );
        }
    }

    /// Run separately, optimized, so process RSS is not mixed with other tests.
    /// BRIDGE_JOURNAL_STRESS_LEGACY=1 reproduces the former storage strategy.
    #[test]
    #[ignore = "standalone journal memory regression: cargo test --release journal_memory -- --ignored --nocapture"]
    fn journal_memory() {
        let dir = TestDir::new();
        let db =
            kv::Store::new(kv::Config::new(dir.join("chain")).cache_capacity(1024 * 1024)).unwrap();
        let view = ChainView::new(db).unwrap();
        let (genesis, mut parent, mut delta) = fixture();
        view.initialize(genesis.header).unwrap();
        delta.adds = vec![delta.adds[0].clone(); 4096];
        delta.changes.clear(); // Isolate journal memory from the UTXO/forest size.
        let legacy = std::env::var_os("BRIDGE_JOURNAL_STRESS_LEGACY").is_some();
        let payload_bytes = serde_json::to_vec(&delta).unwrap().len();
        let mut baseline = 0;
        let rss = || -> u64 {
            let output = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
                * 1024
        };
        for height in 1u32..=128 {
            delta.tip = Tip {
                height,
                block_hash: parent.block_hash(),
            };
            delta.parent = parent.header.prev_blockhash;
            if legacy {
                let mut batch = Batch::new();
                ChainView::metadata(&mut batch, delta.tip, &parent.header, b"roots").unwrap();
                batch
                    .set(
                        &key(b'j', &height.to_be_bytes()),
                        &serde_json::to_vec(&delta).unwrap(),
                    )
                    .unwrap();
                view.data.batch(batch).unwrap();
            } else {
                view.commit(&delta, &parent.header, b"roots").unwrap();
            }
            view.flush().unwrap();
            if height == 32 {
                baseline = rss();
            }
            if height % 32 == 0 {
                println!(
                    "journal_memory legacy={legacy} records={height} payload_bytes={} rss_bytes={}",
                    payload_bytes * height as usize,
                    rss()
                );
            }
            parent = block(&parent, height as u8 + 1, vec![]);
        }
        if !legacy {
            assert!(
                rss().saturating_sub(baseline) < 64 * 1024 * 1024,
                "journal heap grew with retained payloads before pruning"
            );
            for height in 1..=128 {
                view.delta(height).unwrap().unwrap();
            }
            view.prune_journal(128).unwrap();
            let final_rss = rss();
            println!("journal_memory after_replay_and_prune rss_bytes={final_rss}");
            assert!(
                final_rss.saturating_sub(baseline) < 64 * 1024 * 1024,
                "journal heap grew with retained payloads: baseline={baseline}, final={final_rss}"
            );
            assert_eq!(
                std::fs::read_dir(dir.join("chain/journal"))
                    .unwrap()
                    .count(),
                0
            );
        }
    }
}
