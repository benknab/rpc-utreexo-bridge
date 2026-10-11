use std::fs::DirBuilder;
use std::path::PathBuf;

use bitcoin::BlockHash;
use rustreexo::accumulator::mem_forest::MemForest;
use rustreexo::accumulator::proof::Proof;
use serde::Serialize;

use crate::prover::AccumulatorHash;
use crate::prover::BlockStorage;
use crate::udata::shinigami_udata::PoseidonHash;
use crate::udata::LeafContext;

#[derive(Debug, Serialize)]
pub struct UtreexoState {
    roots: Vec<PoseidonHash>,
    num_leaves: u64,
}

#[derive(Debug, Serialize)]
pub struct BlockData {
    block_height: u32,
    utreexo_state: UtreexoState,
    inclusion_proof: Proof<AccumulatorHash>,
}

pub struct JsonBlockFiles {
    dir: PathBuf,
    granularity: u32,
}

impl JsonBlockFiles {
    pub fn new(dir: PathBuf, granularity: u32) -> Self {
        Self { dir, granularity }
    }
}

impl BlockStorage for JsonBlockFiles {
    fn save_block(
        &mut self,
        _hash: BlockHash,
        block_height: u32,
        proof: Proof<AccumulatorHash>,
        _leaves: Vec<LeafContext>,
        acc: &MemForest<AccumulatorHash>,
    ) -> anyhow::Result<()> {
        let block_data = BlockData {
            block_height,
            utreexo_state: UtreexoState {
                roots: acc.get_roots().iter().map(|x| x.get_data()).collect(),
                num_leaves: acc.leaves,
            },
            inclusion_proof: proof,
        };

        let subdir_name = block_height - (block_height % self.granularity);
        let file_path = self.dir.join(subdir_name.to_string());

        DirBuilder::new().recursive(true).create(&file_path)?;

        let file_path = file_path.join(format!("{}.json", block_height));
        crate::checkpoint::atomic_write(&file_path, |writer| {
            serde_json::to_writer(writer, &block_data)?;
            Ok(())
        })
    }
}
