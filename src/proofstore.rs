// SPDX-License-Identifier: MIT
//! Bounded, independently addressable proof records. No Bitcoin block payloads.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use bitcoin::consensus::{deserialize, serialize, Decodable};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, VarInt};
use rustreexo::accumulator::{mem_forest::MemForest, proof::Proof};

use crate::checkpoint::atomic_write;
use crate::prover::{AccumulatorHash, BlockStorage};
use crate::udata::{BatchProof, CompactLeafData, LeafContext, ScriptPubkeyType, UData};

pub struct ProofStore {
    directory: PathBuf,
    retention: u32,
    entries: BTreeMap<u32, BlockHash>,
}

impl ProofStore {
    pub fn new(directory: PathBuf, retention: u32) -> Result<Self> {
        ensure!(retention > 0, "proof retention must be positive");
        fs::create_dir_all(&directory)?;
        let mut entries = BTreeMap::new();
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "proof") {
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("invalid proof filename")?;
                let (height, hash) = name.split_once('-').context("invalid proof filename")?;
                ensure!(
                    entries.insert(height.parse()?, hash.parse()?).is_none(),
                    "duplicate proof height"
                );
            } else if path.extension().is_some_and(|ext| ext == "tmp") {
                fs::remove_file(path)?; // interrupted atomic write, never published
            }
        }
        Ok(Self {
            directory,
            retention,
            entries,
        })
    }

    fn path(&self, height: u32, hash: BlockHash) -> PathBuf {
        self.directory.join(format!("{height:010}-{hash}.proof"))
    }

    pub fn get(&self, height: u32, hash: BlockHash) -> Result<Option<UData>> {
        if self.entries.get(&height) != Some(&hash) {
            return Ok(None);
        }
        let record = fs::read(self.path(height, hash))?;
        let mut reader = record.as_slice();
        ensure!(
            u32::consensus_decode(&mut reader)? == height
                && BlockHash::consensus_decode(&mut reader)? == hash,
            "proof record identity mismatch"
        );
        Ok(Some(deserialize(reader)?))
    }

    pub fn covers(&self, height: u32, count: u32) -> bool {
        (height.saturating_sub(count.saturating_sub(1)).max(1)..=height)
            .all(|h| self.entries.contains_key(&h))
    }

    /// Remove both expired history and stale future records after rollback.
    fn remove_outside(&mut self, height: u32) -> Result<()> {
        let first = height.saturating_sub(self.retention - 1).max(1);
        let expired = self
            .entries
            .iter()
            .filter(|(h, _)| **h < first || **h > height)
            .map(|(h, hash)| (*h, *hash))
            .collect::<Vec<_>>();
        for (h, hash) in expired {
            fs::remove_file(self.path(h, hash))?;
            self.entries.remove(&h);
        }
        fs::File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub fn put(&mut self, height: u32, hash: BlockHash, data: &UData) -> Result<()> {
        // On a fork, removal before publication is safe: the old proof is not
        // needed to recover the accumulator. It also prevents duplicate filenames.
        if let Some(previous) = self.entries.remove(&height) {
            fs::remove_file(self.path(height, previous))?;
        }
        atomic_write(&self.path(height, hash), |writer| {
            use std::io::Write;
            writer.write_all(&height.to_le_bytes())?;
            writer.write_all(&serialize(&hash))?;
            writer.write_all(&serialize(data))?;
            Ok(())
        })?;
        self.entries.insert(height, hash);
        Ok(())
    }
}

impl BlockStorage for ProofStore {
    fn save_block(
        &mut self,
        hash: BlockHash,
        height: u32,
        proof: Proof<AccumulatorHash>,
        leaves: Vec<LeafContext>,
        _acc: &MemForest<AccumulatorHash>,
    ) -> Result<()> {
        let data = UData {
            remember_idx: Vec::new(),
            proof: BatchProof {
                targets: proof.targets.into_iter().map(VarInt).collect(),
                hashes: proof
                    .hashes
                    .into_iter()
                    .map(|h| BlockHash::from_byte_array(*h))
                    .collect(),
            },
            leaves: leaves
                .into_iter()
                .map(|leaf| CompactLeafData {
                    header_code: (leaf.block_height << 1) | u32::from(leaf.is_coinbase),
                    amount: leaf.value,
                    spk_ty: ScriptPubkeyType::from_script(&leaf.pk_script),
                })
                .collect(),
        };
        self.put(height, hash, &data)
    }

    fn prune(&mut self, height: u32) -> Result<()> {
        self.remove_outside(height)
    }

    #[cfg(feature = "api")]
    fn get_proof(&self, height: u32, hash: BlockHash) -> Result<Option<UData>> {
        self.get(height, hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn prunes_on_disk_and_reopens_proof_only_records() {
        let dir = TestDir::new();
        let mut store = ProofStore::new(dir.join("proofs"), 2).unwrap();
        for height in 1..=4 {
            store
                .put(
                    height,
                    BlockHash::from_byte_array([height as u8; 32]),
                    &UData::default(),
                )
                .unwrap();
            store.prune(height).unwrap();
        }
        drop(store);
        let mut store = ProofStore::new(dir.join("proofs"), 2).unwrap();
        assert!(store
            .get(2, BlockHash::from_byte_array([2; 32]))
            .unwrap()
            .is_none());
        assert_eq!(
            store.get(3, BlockHash::from_byte_array([3; 32])).unwrap(),
            Some(UData::default())
        );
        assert_eq!(fs::read_dir(dir.join("proofs")).unwrap().count(), 2);
        // 4-byte height + 32-byte identity + four empty CompactSize vectors.
        assert_eq!(
            fs::metadata(store.path(3, BlockHash::from_byte_array([3; 32])))
                .unwrap()
                .len(),
            40
        );
        let replacement = BlockHash::from_byte_array([9; 32]);
        store.put(3, replacement, &UData::default()).unwrap();
        store.prune(3).unwrap();
        assert!(store
            .get(3, BlockHash::from_byte_array([3; 32]))
            .unwrap()
            .is_none());
        assert!(store.get(3, replacement).unwrap().is_some());
        assert_eq!(fs::read_dir(dir.join("proofs")).unwrap().count(), 1);
    }
}
