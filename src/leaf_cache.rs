use std::sync::Arc;

use anyhow::Result;
use bitcoin::OutPoint;

use crate::chainview::{ChainView, LeafChange};
use crate::prover::LeafCache;
use crate::udata::LeafContext;

/// Leaves share the chain view's atomic commit, so no partially processed block
/// or query can flush or delete a coin independently of its accumulator state.
pub struct DiskLeafStorage {
    view: Arc<ChainView>,
}

impl DiskLeafStorage {
    pub fn new(view: Arc<ChainView>) -> Self {
        Self { view }
    }
}

impl LeafCache for DiskLeafStorage {
    fn get(&self, outpoint: &OutPoint) -> Result<Option<LeafContext>> {
        self.view.leaf(outpoint)
    }

    fn apply(&mut self, _changes: &[LeafChange]) {}
}
