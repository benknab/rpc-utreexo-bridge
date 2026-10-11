// SPDX-License-Identifier: MIT

//! Serial accumulator owner. Requests run between complete block commits.
#[cfg(feature = "memory-leaf-map")]
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::{mpsc::Sender, Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[cfg(feature = "api")]
use crate::state::State;
use anyhow::{ensure, Context, Result};
#[cfg(feature = "api")]
use bitcoin::consensus::serialize;
use bitcoin::{Block, BlockHash, OutPoint, Script};
#[cfg(feature = "api")]
use bitcoin::{Transaction, TxOut, Txid};
#[cfg(feature = "api")]
use futures::channel::mpsc::Receiver;
use log::{error, info};
#[cfg(feature = "api")]
use rustreexo::accumulator::node_hash::BitcoinNodeHash;
use rustreexo::accumulator::{mem_forest::MemForest, proof::Proof};
#[cfg(feature = "api")]
use serde::{Deserialize, Serialize};

use crate::chaininterface::Blockchain;
use crate::chainview::{ChainView, Delta, LeafChange, Tip};
use crate::checkpoint::{acc_bytes, Checkpoints};
use crate::udata::{LeafContext, LeafData};
#[cfg(feature = "api")]
use crate::udata::{UData, UtreexoBlock};

#[cfg(not(feature = "shinigami"))]
pub type AccumulatorHash = rustreexo::accumulator::node_hash::BitcoinNodeHash;
#[cfg(feature = "shinigami")]
pub type AccumulatorHash = crate::udata::shinigami_udata::PoseidonHash;

pub trait BlockStorage {
    fn save_block(
        &mut self,
        hash: BlockHash,
        height: u32,
        proof: Proof<AccumulatorHash>,
        leaves: Vec<LeafContext>,
        acc: &MemForest<AccumulatorHash>,
    ) -> Result<()>;
    fn prune(&mut self, _height: u32) -> Result<()> {
        Ok(())
    }
    #[cfg(feature = "api")]
    fn get_proof(&self, height: u32, hash: BlockHash) -> Result<Option<UData>>;
}

pub trait LeafCache {
    fn get(&self, outpoint: &OutPoint) -> Result<Option<LeafContext>>;
    fn apply(&mut self, changes: &[LeafChange]);
    fn reload(&mut self, _view: &ChainView) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "memory-leaf-map")]
impl LeafCache for HashMap<OutPoint, LeafContext> {
    fn get(&self, outpoint: &OutPoint) -> Result<Option<LeafContext>> {
        Ok(self.get(outpoint).cloned())
    }
    fn apply(&mut self, changes: &[LeafChange]) {
        for change in changes {
            match &change.after {
                Some(leaf) => {
                    self.insert(change.outpoint, leaf.clone());
                }
                None => {
                    self.remove(&change.outpoint);
                }
            }
        }
    }
    fn reload(&mut self, view: &ChainView) -> Result<()> {
        *self = view.all_leaves()?;
        Ok(())
    }
}

pub struct Prover<LeafStorage: LeafCache, Storage: BlockStorage> {
    files: Arc<RwLock<Storage>>,
    rpc: Box<dyn Blockchain>,
    acc: MemForest<AccumulatorHash>,
    tip: Tip,
    view: Arc<ChainView>,
    leaf_data: LeafStorage,
    checkpoints: Checkpoints,
    checkpoint_interval: u32,
    shutdown_flag: Arc<Mutex<bool>>,
    save_proofs_after: u32,
    block_notification: Sender<BlockHash>,
}

impl<L: LeafCache, S: BlockStorage> Prover<L, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc: Box<dyn Blockchain>,
        files: Arc<RwLock<S>>,
        view: Arc<ChainView>,
        mut leaf_data: L,
        start_acc: Option<PathBuf>,
        start_height: Option<u32>,
        checkpoint_interval: Option<u32>,
        shutdown_flag: Arc<Mutex<bool>>,
        save_proofs_after: u32,
        block_notification: Sender<BlockHash>,
    ) -> Result<Self> {
        ensure!(start_acc.is_none() && start_height.is_none(),
            "legacy accumulator-only imports lack an atomic leaf index; use a fresh DATA_DIR and sync from genesis");
        ensure!(
            !PathBuf::from(crate::subdir("pollard")).exists(),
            "legacy bridge storage detected; preserve it and sync using a fresh DATA_DIR"
        );
        let genesis = rpc.get_block_hash(0)?;
        view.initialize(rpc.get_block_header(genesis)?)?;
        let tip = view.tip()?.context("missing committed tip")?;
        let checkpoints = Checkpoints::new(crate::subdir("checkpoints").into())?;
        let acc = checkpoints.restore(&view, tip)?;
        files
            .write()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
            .prune(tip.height)?;
        if tip.height == 0 {
            checkpoints.save(&view, tip, &acc)?;
        }
        let checkpoint_interval = checkpoint_interval.unwrap_or(1000);
        ensure!(
            checkpoint_interval > 0,
            "checkpoint interval must be positive"
        );
        leaf_data.reload(&view)?;
        info!(
            "Restored accumulator at height={} hash={}",
            tip.height, tip.block_hash
        );
        Ok(Self {
            rpc,
            files,
            view,
            leaf_data,
            tip,
            acc,
            checkpoints,
            checkpoint_interval,
            shutdown_flag,
            save_proofs_after,
            block_notification,
        })
    }

    #[cfg(feature = "api")]
    fn handle_request(&mut self, request: Requests) -> Result<Responses> {
        match request {
            Requests::GetProof(hash) => Ok(Responses::Proof(
                self.acc.prove(&[hash]).map_err(anyhow::Error::msg)?,
                State::from_forest(self.tip, &self.acc),
            )),
            Requests::GetRoots => Ok(Responses::Roots(
                self.acc.get_roots().iter().map(|r| r.get_data()).collect(),
            )),
            Requests::GetCSN => Ok(Responses::Csn(State::from_forest(self.tip, &self.acc))),
            Requests::GetWitness(outpoint) => {
                let leaf = self.lookup_leaf(&outpoint)?;
                let hash = LeafData::get_leaf_hashes(&leaf);
                let proof = self.acc.prove(&[hash]).map_err(anyhow::Error::msg)?;
                Ok(Responses::Witness(
                    leaf,
                    proof,
                    State::from_forest(self.tip, &self.acc),
                ))
            }
            Requests::GetLeaf(outpoint) => Ok(Responses::LeafData(self.lookup_leaf(&outpoint)?)),
            Requests::GetBlockByHeight(height) => Ok(Responses::Block(self.block_at(height)?)),
            Requests::GetBlocksByHeight(height, count) => {
                ensure!(count <= 16, "at most 16 blocks per request");
                let end = height.checked_add(count).context("height overflow")?;
                let mut blocks = Vec::new();
                for h in height..end {
                    if h > self.tip.height {
                        break;
                    }
                    blocks.push(self.block_at(h)?);
                }
                Ok(Responses::Blocks(blocks))
            }
            Requests::GetTransaction(txid) | Requests::GetTxUnpent(txid) => {
                let only_unspent = matches!(request, Requests::GetTxUnpent(_));
                let tx = self.rpc.get_transaction(txid)?;
                let mut hashes = Vec::new();
                let mut outputs = Vec::new();
                for (vout, output) in tx.output.iter().enumerate() {
                    if Self::is_unspendable(&output.script_pubkey) {
                        continue;
                    }
                    let outpoint = OutPoint {
                        txid,
                        vout: vout as u32,
                    };
                    if let Some(leaf) = self.leaf_data.get(&outpoint)? {
                        hashes.push(LeafData::get_leaf_hashes(&leaf));
                        outputs.push(output.clone());
                    } else if !only_unspent {
                        anyhow::bail!(
                            "output {outpoint} is not unspent at {}",
                            self.tip.block_hash
                        );
                    }
                }
                let proof = self.acc.prove(&hashes).map_err(anyhow::Error::msg)?;
                if only_unspent {
                    Ok(Responses::TransactionOut(
                        outputs,
                        proof,
                        State::from_forest(self.tip, &self.acc),
                    ))
                } else {
                    Ok(Responses::Transaction(
                        (tx, proof),
                        State::from_forest(self.tip, &self.acc),
                    ))
                }
            }
        }
    }

    fn lookup_leaf(&self, outpoint: &OutPoint) -> Result<LeafContext> {
        self.leaf_data.get(outpoint)?.with_context(|| {
            format!(
                "outpoint {outpoint} is not unspent at {}",
                self.tip.block_hash
            )
        })
    }

    #[cfg(feature = "api")]
    fn block_at(&self, height: u32) -> Result<Vec<u8>> {
        ensure!(height <= self.tip.height, "block is ahead of processed tip");
        let hash = self
            .view
            .get_block_hash(height)?
            .context("unknown block height")?;
        let udata = self
            .files
            .read()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
            .get_proof(height, hash)?
            .context("block proof is pruned or unavailable")?;
        let block = self.rpc.get_block(hash)?;
        ensure!(block.block_hash() == hash, "backend returned wrong block");
        Ok(serialize(&UtreexoBlock {
            block,
            udata: Some(udata),
        }))
    }

    pub fn keep_up(
        &mut self,
        #[cfg(feature = "api")] mut receiver: Receiver<(
            Requests,
            futures::channel::oneshot::Sender<Result<Responses, String>>,
        )>,
    ) -> Result<()> {
        let mut target = self.tip.height;
        let mut next_poll = Instant::now();
        loop {
            if *self.shutdown_flag.lock().unwrap() {
                self.view.flush()?;
                return Ok(());
            }
            #[cfg(feature = "api")]
            for _ in 0..32 {
                let Ok(Some((request, response))) = receiver.try_next() else {
                    break;
                };
                // A disconnected HTTP client must not terminate the prover.
                let _ = response.send(self.handle_request(request).map_err(|e| e.to_string()));
            }
            if Instant::now() >= next_poll {
                next_poll = Instant::now() + Duration::from_secs(10);
                match self.rpc.get_block_count() {
                    Ok(height) => {
                        target = u32::try_from(height)?;
                        self.reconcile(target)?;
                    }
                    Err(e) => {
                        error!("Backend tip unavailable: {e}");
                        target = self.tip.height;
                    }
                }
            }
            if self.tip.height < target {
                let height = self.tip.height + 1;
                let fetched = self.rpc.get_block_hash(u64::from(height)).and_then(|hash| {
                    let block = self.rpc.get_block(hash)?;
                    ensure!(block.block_hash() == hash, "backend returned wrong block");
                    Ok(block)
                });
                let block = match fetched {
                    Ok(block) => block,
                    Err(e) => {
                        error!("Backend block unavailable: {e}");
                        target = self.tip.height;
                        continue;
                    }
                };
                if block.header.prev_blockhash != self.tip.block_hash {
                    next_poll = Instant::now();
                    target = self.tip.height;
                    continue;
                }
                let mtp = self.rpc.get_mtp(self.tip.block_hash)?;
                self.connect_block(&block, mtp)?;
                if self.tip.height == target {
                    let _ = self.block_notification.send(self.tip.block_hash);
                    self.view.flush()?;
                }
            } else {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }

    /// Detect longer, equal-height and rollback-only reorganizations by hash.
    fn reconcile(&mut self, backend_height: u32) -> Result<()> {
        let mut height = self.tip.height.min(backend_height);
        while self.view.get_block_hash(height)? != Some(self.rpc.get_block_hash(u64::from(height))?)
        {
            ensure!(height > 0, "backend genesis changed");
            height -= 1;
        }
        if height == self.tip.height {
            return Ok(());
        }
        let ancestor = Tip {
            height,
            block_hash: self
                .view
                .get_block_hash(height)?
                .context("missing ancestor")?,
        };
        // Check recovery coverage before modifying the committed chain.
        let acc = self.checkpoints.restore(&self.view, ancestor)?;
        for disconnect in height + 1..=self.tip.height {
            let delta = self.view.delta(disconnect)?.context(
                "reorg exceeds retained recovery journal; rebuild with a fresh DATA_DIR",
            )?;
            ensure!(
                self.view.get_block_hash(disconnect)? == Some(delta.tip.block_hash)
                    && self.view.get_block_hash(disconnect - 1)? == Some(delta.parent),
                "recovery journal identity mismatch"
            );
        }
        info!("Reorganizing from {} to {}", self.tip.height, height);
        while self.view.tip()?.context("missing tip")?.height > height {
            self.view.undo_tip()?;
        }
        self.view.flush()?;
        self.checkpoints.discard_after(&self.view, height)?;
        self.files
            .write()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
            .prune(height)?;
        self.tip = ancestor;
        self.acc = acc;
        self.leaf_data.reload(&self.view)?;
        Ok(())
    }

    fn connect_block(&mut self, block: &Block, mtp: u32) -> Result<()> {
        ensure!(
            block.header.prev_blockhash == self.tip.block_hash,
            "block parent mismatch"
        );
        let delta = self.prepare(block, mtp)?;
        let adds = delta
            .adds
            .iter()
            .map(LeafData::get_leaf_hashes)
            .collect::<Vec<_>>();
        let dels = delta
            .dels
            .iter()
            .map(LeafData::get_leaf_hashes)
            .collect::<Vec<_>>();
        let proof = self.acc.prove(&dels).map_err(anyhow::Error::msg)?;
        self.acc.modify(&adds, &dels).map_err(anyhow::Error::msg)?;
        if delta.tip.height > self.save_proofs_after {
            self.files
                .write()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
                .save_block(
                    delta.tip.block_hash,
                    delta.tip.height,
                    proof,
                    delta.dels.clone(),
                    &self.acc,
                )?;
        }
        // Any error after forest modification is fatal to this process. Restart
        // reconstructs from the committed DB and never checkpoints partial work.
        self.view
            .commit(&delta, &block.header, &acc_bytes(&self.acc))?;
        self.leaf_data.apply(&delta.changes);
        self.tip = delta.tip;
        // Do not prune a proof that a durable (older) tip might still require.
        self.view.flush()?;
        self.files
            .write()
            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
            .prune(self.tip.height)?;
        if self.tip.height % self.checkpoint_interval == 0 {
            self.checkpoints.save(&self.view, self.tip, &self.acc)?;
        }
        info!(
            "processed height={} txs={}",
            self.tip.height,
            block.txdata.len()
        );
        Ok(())
    }

    /// Stage leaf changes without touching persistent or accumulator state.
    fn prepare(&self, block: &Block, mtp: u32) -> Result<Delta> {
        let tip = Tip {
            height: self.tip.height.checked_add(1).context("height overflow")?,
            block_hash: block.block_hash(),
        };
        let mut changes: BTreeMap<OutPoint, LeafChange> = BTreeMap::new();
        let mut created = Vec::new();
        let mut consumed = HashSet::new();
        let mut spent = HashSet::new();
        let mut dels = Vec::new();
        for tx in &block.txdata {
            if !tx.is_coinbase() {
                for input in &tx.input {
                    let op = input.previous_output;
                    ensure!(spent.insert(op), "duplicate spend in backend block");
                    let leaf = match changes.get(&op) {
                        Some(change) => change.after.clone().context("already spent in block")?,
                        None => self.lookup_leaf(&op)?,
                    };
                    if leaf.block_hash == tip.block_hash {
                        consumed.insert(op);
                    } else {
                        dels.push(leaf.clone());
                    }
                    changes
                        .entry(op)
                        .or_insert(LeafChange {
                            outpoint: op,
                            before: Some(leaf),
                            after: None,
                        })
                        .after = None;
                }
            }
            let txid = tx.compute_txid();
            for (vout, output) in tx.output.iter().enumerate() {
                if Self::is_unspendable(&output.script_pubkey) {
                    continue;
                }
                let op = OutPoint {
                    txid,
                    vout: vout as u32,
                };
                let leaf = LeafContext {
                    block_hash: tip.block_hash,
                    txid,
                    vout: op.vout,
                    value: output.value.to_sat(),
                    pk_script: output.script_pubkey.clone(),
                    block_height: tip.height,
                    median_time_past: mtp,
                    is_coinbase: tx.is_coinbase(),
                };
                let before = self.leaf_data.get(&op)?;
                // BIP30's two historical overwrites leave the original
                // block-bound hash in Utreexo, but replace the outpoint index.
                changes
                    .entry(op)
                    .or_insert(LeafChange {
                        outpoint: op,
                        before,
                        after: None,
                    })
                    .after = Some(leaf.clone());
                created.push((op, leaf));
            }
        }
        let adds = created
            .into_iter()
            .filter(|(op, _)| !consumed.contains(op))
            .map(|(_, leaf)| leaf)
            .collect();
        Ok(Delta {
            tip,
            parent: self.tip.block_hash,
            adds,
            dels,
            changes: changes.into_values().collect(),
        })
    }

    fn is_unspendable(script: &Script) -> bool {
        script.len() > 10_000 || script.as_bytes().first() == Some(&0x6a)
    }
}

#[cfg(feature = "api")]
pub enum Requests {
    GetProof(BitcoinNodeHash),
    GetRoots,
    GetBlockByHeight(u32),
    GetTransaction(Txid),
    GetCSN,
    GetBlocksByHeight(u32, u32),
    GetTxUnpent(Txid),
    GetLeaf(OutPoint),
    GetWitness(OutPoint),
}

#[cfg(feature = "api")]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Responses {
    Proof(Proof, State),
    Roots(Vec<BitcoinNodeHash>),
    Block(Vec<u8>),
    Transaction((Transaction, Proof), State),
    Csn(State),
    Blocks(Vec<Vec<u8>>),
    TransactionOut(Vec<TxOut>, Proof, State),
    LeafData(LeafContext),
    Witness(LeafContext, Proof, State),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leaf_cache::DiskLeafStorage;
    use crate::test_support::{block, outpoint, store, transaction, TestChain, TestDir};

    struct NoBlocks;
    impl BlockStorage for NoBlocks {
        fn save_block(
            &mut self,
            _: BlockHash,
            _: u32,
            _: Proof<AccumulatorHash>,
            _: Vec<LeafContext>,
            _: &MemForest<AccumulatorHash>,
        ) -> Result<()> {
            Ok(())
        }
        #[cfg(feature = "api")]
        fn get_proof(&self, _: u32, _: BlockHash) -> Result<Option<UData>> {
            Ok(None)
        }
    }

    fn prover(dir: &TestDir, chain: &TestChain) -> Prover<DiskLeafStorage, NoBlocks> {
        let view = Arc::new(ChainView::new(store(&dir.join("chain"))));
        view.initialize(chain.0.read().unwrap()[0].header).unwrap();
        let tip = view.tip().unwrap().unwrap();
        let checkpoints = Checkpoints::new(dir.join("checkpoints")).unwrap();
        let acc = checkpoints.restore(&view, tip).unwrap();
        if tip.height == 0 {
            checkpoints.save(&view, tip, &acc).unwrap();
        }
        Prover {
            rpc: Box::new(chain.clone()),
            files: Arc::new(RwLock::new(NoBlocks)),
            acc,
            tip,
            leaf_data: DiskLeafStorage::new(view.clone()),
            view,
            checkpoints,
            checkpoint_interval: 2,
            shutdown_flag: Arc::new(Mutex::new(false)),
            save_proofs_after: u32::MAX,
            block_notification: std::sync::mpsc::channel().0,
        }
    }

    #[test]
    fn restart_reorg_and_read_only_queries_preserve_membership() {
        let dir = TestDir::new();
        let chain = TestChain::new();
        let genesis = chain.0.read().unwrap()[0].clone();
        let first = block(&genesis, 1, vec![]);
        let coin = outpoint(&first.txdata[0]);
        let mut p = prover(&dir, &chain);
        chain.0.write().unwrap().push(first.clone());
        p.connect_block(&first, 0).unwrap();
        let leaf = p.lookup_leaf(&coin).unwrap();
        let hash = LeafData::get_leaf_hashes(&leaf);
        let old_proof = p.acc.prove(&[hash]).unwrap();
        let old_roots = p.view.get_acc(p.tip.block_hash).unwrap().unwrap();
        #[cfg(feature = "api")]
        for _ in 0..3 {
            p.handle_request(Requests::GetTransaction(coin.txid))
                .unwrap();
            p.handle_request(Requests::GetTxUnpent(coin.txid)).unwrap();
            assert_eq!(p.lookup_leaf(&coin).unwrap(), leaf);
        }
        let second = block(&first, 2, vec![transaction(coin, 20)]);
        chain.0.write().unwrap().push(second.clone());
        p.connect_block(&second, 0).unwrap(); // checkpoint
        assert!(p.lookup_leaf(&coin).is_err());
        let third = block(&second, 3, vec![]);
        chain.0.write().unwrap().push(third.clone());
        p.connect_block(&third, 0).unwrap(); // journal only
        let expected = acc_bytes(&p.acc);
        p.view.flush().unwrap();
        drop(p); // no shutdown snapshot
        let mut p = prover(&dir, &chain);
        assert_eq!(p.tip.block_hash, third.block_hash());
        assert_eq!(acc_bytes(&p.acc), expected);
        assert_eq!(
            p.view.get_acc(first.block_hash()).unwrap().unwrap(),
            old_roots
        );
        let past = p
            .checkpoints
            .restore(
                &p.view,
                Tip {
                    height: 1,
                    block_hash: first.block_hash(),
                },
            )
            .unwrap();
        assert!(past.verify(&old_proof, &[hash]).unwrap()); // valid even after spend

        // Replace the tip without increasing the backend height.
        let replacement = block(&second, 4, vec![]);
        chain.0.write().unwrap()[3] = replacement.clone();
        p.reconcile(3).unwrap();
        assert_eq!(p.tip.block_hash, second.block_hash());
        p.connect_block(&replacement, 0).unwrap();
        assert!(p.view.active_tip_for(third.block_hash()).unwrap().is_none());

        // A rollback below the most recent checkpoint restores the spent coin.
        chain.0.write().unwrap().truncate(2);
        p.reconcile(1).unwrap();
        assert_eq!(p.lookup_leaf(&coin).unwrap(), leaf);
        assert!(p.acc.verify(&old_proof, &[hash]).unwrap());
    }

    #[test]
    fn same_block_spends_are_excluded_and_recovery_is_bounded() {
        let dir = TestDir::new();
        let chain = TestChain::new();
        let mut p = prover(&dir, &chain);
        let mut parent = chain.0.read().unwrap()[0].clone();
        let first = block(&parent, 1, vec![]);
        p.connect_block(&first, 0).unwrap();
        let created = transaction(outpoint(&first.txdata[0]), 10);
        let consumed = outpoint(&created);
        let second = block(&first, 2, vec![created, transaction(consumed, 11)]);
        p.connect_block(&second, 0).unwrap();
        assert!(p.lookup_leaf(&consumed).is_err());
        assert_eq!(p.acc.leaves, 3); // first coinbase, second coinbase, final output
        parent = second;
        for height in 3..=6 {
            let next = block(&parent, height, vec![]);
            p.connect_block(&next, 0).unwrap();
            parent = next;
        }
        assert!(p.view.delta(4).unwrap().is_none());
        assert!(p.view.delta(5).unwrap().is_some());
        assert_eq!(
            std::fs::read_dir(dir.join("checkpoints")).unwrap().count(),
            2
        );
        assert!(p.view.get_acc(first.block_hash()).unwrap().is_some());
        let before = p.tip;
        assert!(p.reconcile(0).is_err()); // fails before modifying the active tip
        assert_eq!(p.view.tip().unwrap(), Some(before));
        assert_eq!(
            acc_bytes(&p.checkpoints.restore(&p.view, p.tip).unwrap()),
            acc_bytes(&p.acc)
        );
    }
}
