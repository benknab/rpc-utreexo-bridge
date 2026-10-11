//! Bitcoin Utreexo state encoding and the BIP183 fixed-63-row position boundary.
use anyhow::{ensure, Context, Result};
use rustreexo::accumulator::{mem_forest::MemForest, node_hash::BitcoinNodeHash};
use serde::{Deserialize, Serialize};

use crate::chainview::Tip;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct State {
    #[serde(flatten)]
    pub tip: Tip,
    pub leaves: u64,
    /// Hash bytes in digest/wire order, including all-zero empty roots.
    pub roots: Vec<String>,
}

impl State {
    pub fn from_forest(tip: Tip, forest: &MemForest) -> Self {
        Self {
            tip,
            leaves: forest.leaves,
            roots: forest
                .get_roots()
                .iter()
                .map(|root| hash_hex(root.get_data()))
                .collect(),
        }
    }

    pub fn decode(tip: Tip, bytes: &[u8]) -> Result<Self> {
        let leaves = u64::from_le_bytes(
            bytes
                .get(..8)
                .context("truncated accumulator state")?
                .try_into()?,
        );
        ensure!(leaves < 1u64 << 63, "unsupported forest size");
        ensure!(
            bytes.len() == 8 + 32 * leaves.count_ones() as usize,
            "invalid accumulator root count"
        );
        Ok(Self {
            tip,
            leaves,
            roots: bytes[8..].chunks_exact(32).map(hex::encode).collect(),
        })
    }
}

pub fn hash_hex(hash: BitcoinNodeHash) -> String {
    hex::encode(*hash)
}

pub fn forest_rows(leaves: u64) -> u32 {
    64 - leaves.saturating_sub(1).leading_zeros()
}

/// Promoted leaves live above row zero. Translate their row start, not the
/// numeric position alone. Go Utreexo uses 63 rows; rustreexo 0.4 uses ceil(log2 N).
pub fn target_to_wire(target: u64, leaves: u64) -> Result<u64> {
    ensure!(leaves > 0 && leaves < 1u64 << 63, "unsupported forest size");
    let rows = forest_rows(leaves);
    let start = |row: u32, size: u32| (1u128 << (size + 1)) - (1u128 << (size + 1 - row));
    for row in 0..=rows {
        let first = start(row, rows);
        if u128::from(target) >= first && u128::from(target) - first < 1u128 << (rows - row) {
            return Ok((start(row, 63) + u128::from(target) - first) as u64);
        }
    }
    anyhow::bail!("proof target outside forest")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{hashes::Hash, BlockHash};

    #[test]
    fn matches_utreexod_leaf_roots_and_promoted_membership_vectors() {
        use crate::udata::{LeafContext, LeafData};
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Step {
            adds: Vec<usize>,
            dels: Vec<usize>,
            prove: Vec<usize>,
            leaves: u64,
            roots: Vec<String>,
            proof: Vec<String>,
            targets: Option<Vec<u64>>,
        }
        #[derive(Deserialize)]
        struct Vectors {
            leaf_hashes: Vec<String>,
            steps: Vec<Step>,
        }
        let vectors: Vectors =
            serde_json::from_str(include_str!("../tests/data/utreexo.json")).unwrap();
        let hashes = (0..11)
            .map(|i: u8| {
                LeafData::get_leaf_hashes(&LeafContext {
                    block_hash: BlockHash::from_byte_array(std::array::from_fn(|j| i + j as u8)),
                    txid: bitcoin::Txid::from_byte_array(std::array::from_fn(|j| i + j as u8 + 7)),
                    vout: u32::from(i) * 3,
                    block_height: u32::from(i) + 100,
                    value: u64::from(i) + 1000,
                    is_coinbase: i % 2 == 0,
                    pk_script: bitcoin::ScriptBuf::from_bytes(vec![0x51, i]),
                    median_time_past: 0,
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            hashes.iter().copied().map(hash_hex).collect::<Vec<_>>(),
            vectors.leaf_hashes
        );
        let mut forest = MemForest::new();
        let tip = Tip {
            height: 0,
            block_hash: BlockHash::all_zeros(),
        };
        for step in vectors.steps {
            let select = |indices: &[usize]| indices.iter().map(|i| hashes[*i]).collect::<Vec<_>>();
            forest
                .modify(&select(&step.adds), &select(&step.dels))
                .unwrap();
            let state = State::from_forest(tip, &forest);
            assert_eq!(state.leaves, step.leaves);
            assert_eq!(state.roots, step.roots);
            let proof = forest.prove(&select(&step.prove)).unwrap();
            assert_eq!(
                proof
                    .hashes
                    .iter()
                    .copied()
                    .map(hash_hex)
                    .collect::<Vec<_>>(),
                step.proof
            );
            assert_eq!(
                proof
                    .targets
                    .iter()
                    .map(|t| target_to_wire(*t, state.leaves).unwrap())
                    .collect::<Vec<_>>(),
                step.targets.unwrap_or_default()
            );
            assert!(forest.verify(&proof, &select(&step.prove)).unwrap());
        }
    }

    #[test]
    fn root_decoder_rejects_truncation_and_preserves_empty_roots() {
        let tip = Tip {
            height: 1,
            block_hash: BlockHash::all_zeros(),
        };
        let mut bytes = 3u64.to_le_bytes().to_vec();
        bytes.extend([0; 32]);
        bytes.extend([7; 32]);
        let state = State::decode(tip, &bytes).unwrap();
        assert_eq!(state.roots, vec!["00".repeat(32), "07".repeat(32)]);
        assert!(State::decode(tip, &bytes[..71]).is_err());
        assert!(State::decode(tip, &[0; 7]).is_err());
        assert_eq!(target_to_wire(10, 8).unwrap(), (1u64 << 63) + 2);
        assert_eq!(target_to_wire(12, 8).unwrap(), 3u64 << 62);
        assert_eq!(target_to_wire(0, 1).unwrap(), 0);
        assert!(target_to_wire(15, 8).is_err());
    }
}
