use std::cell::{Cell, RefCell};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::{ensure, Result};
use bitcoin::OutPoint;

use crate::chainview::{ChainView, LeafChange};
use crate::prover::LeafCache;
use crate::udata::LeafContext;

#[derive(Clone, Copy, Debug, Default)]
pub struct ReadStats {
    pub hits: u64,
    pub reads: u64,
    pub db_us: u128,
}

struct Entry {
    outpoint: OutPoint,
    leaf: LeafContext,
}

/// Direct-mapped read-through cache. Fixed slots avoid unbounded eviction lists
/// and hash-table growth; collisions only reduce hit rate. Slot allocation plus
/// owned script bytes stays within the configured budget (allocator overhead is
/// not included). No negative entries or uncommitted coin changes are cached.
struct CoinCache {
    slots: Vec<Option<Entry>>,
    hasher: ahash::RandomState,
    budget: usize,
    bytes: usize,
    hand: usize,
}

impl CoinCache {
    fn new(budget: usize) -> Self {
        let count = budget / (std::mem::size_of::<Option<Entry>>() + 64);
        let slots: Vec<_> = std::iter::repeat_with(|| None).take(count).collect();
        let bytes = slots.capacity() * std::mem::size_of::<Option<Entry>>();
        Self {
            slots,
            hasher: ahash::RandomState::new(),
            budget,
            bytes,
            hand: 0,
        }
    }

    fn index(&self, op: &OutPoint) -> Option<usize> {
        (!self.slots.is_empty()).then(|| self.hasher.hash_one(op) as usize % self.slots.len())
    }

    fn get(&self, op: &OutPoint) -> Option<LeafContext> {
        let entry = self.slots[self.index(op)?].as_ref()?;
        (entry.outpoint == *op).then(|| entry.leaf.clone())
    }

    fn evict(&mut self, index: usize) {
        if let Some(entry) = self.slots[index].take() {
            self.bytes -= entry.leaf.pk_script.len();
        }
    }

    fn remove(&mut self, op: &OutPoint) {
        if let Some(index) = self.index(op) {
            if self.slots[index]
                .as_ref()
                .is_some_and(|e| e.outpoint == *op)
            {
                self.evict(index);
            }
        }
    }

    fn insert(&mut self, op: OutPoint, leaf: &LeafContext) {
        let Some(index) = self.index(&op) else { return };
        self.evict(index);
        let size = leaf.pk_script.len();
        let fixed = self.slots.capacity() * std::mem::size_of::<Option<Entry>>();
        if size > self.budget - fixed {
            return;
        }
        while self.bytes + size > self.budget {
            self.evict(self.hand);
            self.hand = (self.hand + 1) % self.slots.len();
        }
        self.bytes += size;
        self.slots[index] = Some(Entry {
            outpoint: op,
            leaf: leaf.clone(),
        });
    }

    fn clear(&mut self) {
        for index in 0..self.slots.len() {
            self.evict(index);
        }
    }
}

type ReadResult = Result<Vec<(usize, Option<LeafContext>)>>;
struct ReadJob {
    outpoints: Vec<(usize, OutPoint)>,
    reply: mpsc::Sender<(ReadResult, u128)>,
}

struct Reader {
    sender: Option<mpsc::SyncSender<ReadJob>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The owner applies cache changes only after ChainView's atomic block commit.
/// Workers read the same stable pre-block view and all finish before mutation.
pub struct DiskLeafStorage {
    view: Arc<ChainView>,
    cache: RefCell<CoinCache>,
    readers: Vec<Reader>,
    stats: Cell<ReadStats>,
}

impl DiskLeafStorage {
    #[cfg(test)]
    pub fn new(view: Arc<ChainView>) -> Self {
        Self::with_config(view, 64 * 1024, 2).unwrap()
    }

    pub fn with_config(view: Arc<ChainView>, cache_bytes: usize, threads: usize) -> Result<Self> {
        ensure!(threads <= 16, "at most 16 leaf read threads");
        let mut readers = Vec::new();
        for index in 0..threads {
            let (sender, receiver) = mpsc::sync_channel::<ReadJob>(1);
            let view = view.clone();
            let thread = std::thread::Builder::new()
                .name(format!("leaf-read-{index}"))
                .spawn(move || {
                    while let Ok(job) = receiver.recv() {
                        let started = Instant::now();
                        let result = job
                            .outpoints
                            .into_iter()
                            .map(|(i, op)| Ok((i, view.leaf(&op)?)))
                            .collect();
                        let _ = job.reply.send((result, started.elapsed().as_micros()));
                    }
                })?;
            readers.push(Reader {
                sender: Some(sender),
                thread: Some(thread),
            });
        }
        Ok(Self {
            view,
            cache: RefCell::new(CoinCache::new(cache_bytes)),
            readers,
            stats: Cell::new(ReadStats::default()),
        })
    }
}

impl LeafCache for DiskLeafStorage {
    fn get(&self, outpoint: &OutPoint) -> Result<Option<LeafContext>> {
        let mut stats = self.stats.get();
        if let Some(leaf) = self.cache.borrow().get(outpoint) {
            stats.hits += 1;
            self.stats.set(stats);
            return Ok(Some(leaf));
        }
        let started = Instant::now();
        let leaf = self.view.leaf(outpoint)?;
        stats.reads += 1;
        stats.db_us += started.elapsed().as_micros();
        self.stats.set(stats);
        if let Some(leaf) = &leaf {
            self.cache.borrow_mut().insert(*outpoint, leaf);
        }
        Ok(leaf)
    }

    fn get_many(&self, outpoints: &[OutPoint]) -> Result<Vec<Option<LeafContext>>> {
        // Tiny reads are cheaper on the owner than dispatching worker jobs.
        if self.readers.is_empty() || outpoints.len() < 64 {
            return outpoints.iter().map(|op| self.get(op)).collect();
        }
        let mut stats = self.stats.get();
        let mut values = Vec::with_capacity(outpoints.len());
        let mut missing = Vec::new();
        {
            let cache = self.cache.borrow();
            for (i, op) in outpoints.iter().enumerate() {
                let leaf = cache.get(op);
                if leaf.is_some() {
                    stats.hits += 1;
                } else {
                    missing.push((i, *op));
                }
                values.push(leaf);
            }
        }
        stats.reads += missing.len() as u64;
        let (reply, receiver) = mpsc::channel();
        let chunk_size = missing.len().div_ceil(self.readers.len()).max(1);
        // Each block submits at most one bounded job per worker. Dropping the
        // original sender also makes a panicked/disconnected worker observable.
        let mut error = None;
        for (reader, chunk) in self.readers.iter().zip(missing.chunks(chunk_size)) {
            if reader
                .sender
                .as_ref()
                .unwrap()
                .send(ReadJob {
                    outpoints: chunk.to_vec(),
                    reply: reply.clone(),
                })
                .is_err()
            {
                error = Some(anyhow::anyhow!("leaf reader disconnected"));
            }
        }
        drop(reply);
        let mut received = 0;
        for (result, elapsed) in receiver {
            stats.db_us += elapsed; // Sum of overlapping worker times, not wall time.
            match result {
                Ok(leaves) => {
                    received += leaves.len();
                    for (i, leaf) in leaves {
                        // These misses are about to be spent. Do not evict hot
                        // unspent outputs to admit single-use input records.
                        values[i] = leaf;
                    }
                }
                Err(e) => error = Some(e),
            }
        }
        self.stats.set(stats);
        // Drain every submitted read before returning even on error; no worker
        // can still be reading a view when the owner starts its next operation.
        if let Some(error) = error {
            return Err(error.context("fetching block inputs"));
        }
        ensure!(
            received == missing.len(),
            "leaf reader exited without a result"
        );
        Ok(values)
    }

    fn stats(&self) -> ReadStats {
        self.stats.get()
    }

    fn apply(&mut self, changes: &[LeafChange]) {
        let cache = self.cache.get_mut();
        for change in changes {
            match &change.after {
                Some(leaf) => cache.insert(change.outpoint, leaf),
                None => cache.remove(&change.outpoint),
            }
        }
    }

    fn reload(&mut self, _view: &ChainView) -> Result<()> {
        self.cache.get_mut().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chainview::{Delta, Tip};
    use crate::test_support::{block, store, TestChain, TestDir};
    use bitcoin::{hashes::Hash, BlockHash, ScriptBuf, Txid};

    fn leaf(i: u32, script_size: usize) -> LeafContext {
        LeafContext {
            block_hash: BlockHash::from_byte_array([1; 32]),
            txid: Txid::from_byte_array([2; 32]),
            vout: i,
            value: u64::from(i),
            pk_script: ScriptBuf::from_bytes(vec![0x51; script_size]),
            block_height: 1,
            median_time_past: 1,
            is_coinbase: false,
        }
    }

    fn op(leaf: &LeafContext) -> OutPoint {
        OutPoint {
            txid: leaf.txid,
            vout: leaf.vout,
        }
    }

    #[test]
    fn cache_budget_collisions_and_oversized_scripts_preserve_identity() {
        for budget in [0, 1, 512, 4096] {
            let mut cache = CoinCache::new(budget);
            for i in 0..500 {
                let leaf = leaf(i, if i % 3 == 0 { 10_000 } else { 25 });
                cache.insert(op(&leaf), &leaf);
                assert!(cache.bytes <= budget);
                for j in i.saturating_sub(3)..=i {
                    let previous = leaf_for_key(j);
                    if let Some(found) = cache.get(&previous) {
                        assert_eq!(found.vout, j);
                        assert_eq!(found.value, u64::from(j));
                    }
                }
                cache.remove(&op(&leaf));
                assert!(cache.get(&op(&leaf)).is_none());
            }
            cache.clear();
            assert!(cache.slots.iter().all(Option::is_none));
        }
    }

    fn leaf_for_key(i: u32) -> OutPoint {
        op(&leaf(i, 25))
    }

    #[test]
    fn parallel_reads_match_serial_and_cache_tracks_commit_and_undo() {
        let dir = TestDir::new();
        let view = Arc::new(ChainView::new(store(&dir.join("chain"))).unwrap());
        let chain = TestChain::new();
        let genesis = chain.0.read().unwrap()[0].clone();
        view.initialize(genesis.header).unwrap();
        let first = block(&genesis, 1, vec![]);
        let changes: Vec<_> = (0..128)
            .map(|i| {
                let leaf = leaf(i, 25);
                LeafChange {
                    outpoint: op(&leaf),
                    before: None,
                    after: Some(leaf),
                }
            })
            .collect();
        view.commit(
            &Delta {
                tip: Tip {
                    height: 1,
                    block_hash: first.block_hash(),
                },
                parent: genesis.block_hash(),
                adds: vec![],
                dels: vec![],
                changes,
            },
            &first.header,
            &0u64.to_le_bytes(),
        )
        .unwrap();
        let mut parallel = DiskLeafStorage::with_config(view.clone(), 8192, 4).unwrap();
        let serial = DiskLeafStorage::with_config(view.clone(), 0, 0).unwrap();
        let keys: Vec<_> = (0..132).rev().map(leaf_for_key).collect();
        assert_eq!(
            parallel.get_many(&keys).unwrap(),
            serial.get_many(&keys).unwrap()
        );
        let original = parallel.get(&leaf_for_key(1)).unwrap().unwrap();
        parallel.get(&leaf_for_key(1)).unwrap();
        assert_eq!(parallel.stats().hits, 1);
        let second = block(&first, 2, vec![]);
        let mut replacement = leaf(2, 25);
        replacement.value = 999;
        let delta = Delta {
            tip: Tip {
                height: 2,
                block_hash: second.block_hash(),
            },
            parent: first.block_hash(),
            adds: vec![],
            dels: vec![],
            changes: vec![
                LeafChange {
                    outpoint: op(&original),
                    before: Some(original.clone()),
                    after: None,
                },
                LeafChange {
                    outpoint: op(&replacement),
                    before: Some(leaf(2, 25)),
                    after: Some(replacement.clone()),
                },
            ],
        };
        view.commit(&delta, &second.header, &0u64.to_le_bytes())
            .unwrap();
        parallel.apply(&delta.changes);
        assert!(parallel.get(&op(&original)).unwrap().is_none());
        assert_eq!(parallel.get(&op(&replacement)).unwrap(), Some(replacement));
        view.undo_tip().unwrap();
        parallel.reload(&view).unwrap();
        assert_eq!(parallel.get(&op(&original)).unwrap(), Some(original));
        assert_eq!(parallel.get(&leaf_for_key(2)).unwrap(), Some(leaf(2, 25)));
        assert_eq!(
            parallel.get_many(&keys).unwrap(),
            serial.get_many(&keys).unwrap()
        );
    }

    #[test]
    fn disconnected_reader_returns_an_error_after_draining_other_workers() {
        let dir = TestDir::new();
        let view = Arc::new(ChainView::new(store(&dir.join("chain"))).unwrap());
        let mut storage = DiskLeafStorage::with_config(view, 0, 2).unwrap();
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        storage.readers[0].sender = Some(sender);
        let keys: Vec<_> = (0..128).map(leaf_for_key).collect();
        assert!(storage
            .get_many(&keys)
            .unwrap_err()
            .to_string()
            .contains("fetching block inputs"));
    }
}
