// SPDX-License-Identifier: MIT

//! Active-chain metadata, historical roots, leaves and recovery deltas share one
//! atomic bucket. A committed tip always describes a complete block.

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
    /// Serializes publication against multi-field reads by REST and P2P.
    pub(crate) gate: RwLock<()>,
}

fn key(prefix: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = vec![prefix];
    key.extend_from_slice(suffix);
    key
}

impl ChainView {
    pub fn new(storage: Store) -> Self {
        Self {
            data: storage
                .bucket(Some("bridge-v2"))
                .expect("open chain bucket"),
            gate: RwLock::new(()),
        }
    }

    pub fn initialize(&self, header: Header) -> Result<()> {
        let hash = header.block_hash();
        if let Some(genesis) = self.data.get(&b"genesis".to_vec())? {
            ensure!(
                genesis == serialize(&hash),
                "backend network differs from stored genesis"
            );
            ensure!(self.tip()?.is_some(), "missing committed tip");
            return Ok(());
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
        self.data.batch(batch)?;
        self.flush()?;
        Ok(())
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
        let mut batch = Batch::new();
        for change in &delta.changes {
            Self::write_leaf(&mut batch, change.outpoint, change.after.as_ref())?;
        }
        Self::metadata(&mut batch, delta.tip, header, roots)?;
        batch.set(
            &key(b'j', &delta.tip.height.to_be_bytes()),
            &serde_json::to_vec(delta)?,
        )?;
        self.data.batch(batch)?;
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
        self.data.batch(batch)?;
        Ok(previous)
    }

    pub fn delta(&self, height: u32) -> Result<Option<Delta>> {
        self.data
            .get(&key(b'j', &height.to_be_bytes()))?
            .map(|v| Ok(serde_json::from_slice(&v)?))
            .transpose()
    }

    pub fn prune_journal(&self, through: u32) -> Result<()> {
        let mut batch = Batch::new();
        for item in self.data.iter_prefix(&vec![b'j'])? {
            let item = item?;
            let k: Vec<u8> = item.key()?;
            let height = u32::from_be_bytes(k[1..].try_into()?);
            if height > through {
                break;
            }
            batch.remove(&k)?;
        }
        self.data.batch(batch)?;
        Ok(())
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
