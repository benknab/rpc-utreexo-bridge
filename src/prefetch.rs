// SPDX-License-Identifier: MIT

//! One download worker overlaps Core I/O with serial accumulator commits.
//! Only two queued blocks plus one in-flight download; no disk cache.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use bitcoin::Block;

use crate::chaininterface::Blockchain;

pub struct FetchedBlock {
    pub height: u32,
    pub block: Block,
    pub fetch_us: u128,
}

pub enum Next {
    Block(FetchedBlock),
    Pending,
    Finished,
}

pub struct Prefetch {
    receiver: Option<mpsc::Receiver<Result<FetchedBlock>>>,
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Prefetch {
    pub fn new(backend: Arc<dyn Blockchain>, start: u32, end: u32) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(2);
        let cancel = Arc::new(AtomicBool::new(false));
        let stop = cancel.clone();
        let worker = std::thread::Builder::new()
            .name("block-prefetch".into())
            .spawn(move || {
                let run = || -> Result<()> {
                    let mut height = u64::from(start);
                    while height <= u64::from(end) && !stop.load(Ordering::Relaxed) {
                        let mut started = Instant::now();
                        let count = (u64::from(end) - height + 1).min(128) as u32;
                        let hashes = backend.get_block_hashes(height as u32, count)?;
                        ensure!(
                            !hashes.is_empty() && hashes.len() <= count as usize,
                            "invalid backend header batch"
                        );
                        for hash in hashes {
                            if stop.load(Ordering::Relaxed) {
                                return Ok(());
                            }
                            let block = backend.get_block(hash)?;
                            ensure!(block.block_hash() == hash, "backend returned wrong block");
                            let fetched = FetchedBlock {
                                height: height as u32,
                                block,
                                fetch_us: started.elapsed().as_micros(),
                            };
                            if sender.send(Ok(fetched)).is_err() {
                                return Ok(());
                            }
                            height += 1;
                            started = Instant::now();
                        }
                    }
                    Ok(())
                };
                if let Err(error) = run() {
                    let _ = sender.send(Err(error));
                }
            })?;
        Ok(Self {
            receiver: Some(receiver),
            cancel,
            worker: Some(worker),
        })
    }

    /// Short waits let the owner service witnesses and shutdown while Core is slow.
    pub fn next(&self) -> Result<Next> {
        match self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(Duration::from_millis(20))
        {
            Ok(result) => result.map(Next::Block),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(Next::Pending),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(Next::Finished),
        }
    }
}

impl Drop for Prefetch {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        // Unblock a producer waiting on a full queue before joining it. No old
        // worker survives cancellation, so repeated reorgs cannot grow buffers.
        self.receiver.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{block, TestChain};

    #[test]
    fn ordered_bounded_range_and_full_queue_cancellation() {
        let chain = TestChain::new();
        {
            let mut blocks = chain.0.write().unwrap();
            for tag in 1..=150 {
                let next = block(blocks.last().unwrap(), tag, vec![]);
                blocks.push(next);
            }
        }
        let backend: Arc<dyn Blockchain> = Arc::new(chain.clone());
        let fetch = Prefetch::new(backend.clone(), 1, 130).unwrap();
        let mut height = 0;
        loop {
            match fetch.next().unwrap() {
                Next::Block(fetched) => {
                    height += 1;
                    assert_eq!(fetched.height, height);
                    assert_eq!(
                        fetched.block.block_hash(),
                        chain.get_block_hash(u64::from(height)).unwrap()
                    );
                }
                Next::Pending => (),
                Next::Finished => break,
            }
        }
        assert_eq!(height, 130);
        let fetch = Prefetch::new(backend, 1, 150).unwrap();
        // Wait until the two slots are filled by observing that the producer
        // remains live; dropping the receiver must unblock its send and join.
        std::thread::sleep(Duration::from_millis(50));
        assert!(!fetch.worker.as_ref().unwrap().is_finished());
        drop(fetch);
    }
}
