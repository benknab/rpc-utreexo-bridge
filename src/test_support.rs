//! Synthetic trusted-chain fixtures for persistence and fork tests.
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use anyhow::{Context, Result};
use bitcoin::{
    absolute, transaction, Amount, Block, BlockHash, Network, OutPoint, ScriptBuf, Sequence,
    Transaction, TxIn, TxOut, Txid, Witness,
};

use crate::chaininterface::{Blockchain, TransactionInfo};

pub struct TestDir(pub PathBuf);
impl TestDir {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "bridge-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    pub fn join(&self, path: &str) -> PathBuf {
        self.0.join(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn store(path: &Path) -> kv::Store {
    kv::Store::new(kv::Config {
        path: path.into(),
        temporary: false,
        use_compression: false,
        flush_every_ms: None,
        cache_capacity: None,
        segment_size: None,
    })
    .unwrap()
}

#[derive(Clone)]
pub struct TestChain(pub Arc<RwLock<Vec<Block>>>, pub Option<Arc<FetchGate>>);

#[derive(Default)]
pub struct FetchGate {
    pub state: Mutex<(bool, bool)>, // entered, released
    pub changed: Condvar,
}
impl TestChain {
    pub fn new() -> Self {
        Self(
            Arc::new(RwLock::new(vec![bitcoin::constants::genesis_block(
                Network::Regtest,
            )])),
            None,
        )
    }
}
impl Blockchain for TestChain {
    fn get_block(&self, hash: BlockHash) -> Result<Block> {
        if let Some(gate) = &self.1 {
            let mut state = gate.state.lock().unwrap();
            state.0 = true;
            gate.changed.notify_all();
            // A timeout keeps a failing test from leaking a blocked thread.
            let (state, _) = gate
                .changed
                .wait_timeout_while(state, std::time::Duration::from_secs(10), |s| !s.1)
                .unwrap();
            anyhow::ensure!(state.1, "test fetch was not released");
        }
        self.0
            .read()
            .unwrap()
            .iter()
            .find(|b| b.block_hash() == hash)
            .cloned()
            .context("unknown block")
    }
    fn get_transaction(&self, txid: Txid) -> Result<Transaction> {
        self.0
            .read()
            .unwrap()
            .iter()
            .flat_map(|b| &b.txdata)
            .find(|tx| tx.compute_txid() == txid)
            .cloned()
            .context("unknown tx")
    }
    fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        self.0
            .read()
            .unwrap()
            .get(height as usize)
            .map(Block::block_hash)
            .context("unknown height")
    }
    fn get_block_height(&self, hash: BlockHash) -> Result<u32> {
        self.0
            .read()
            .unwrap()
            .iter()
            .position(|b| b.block_hash() == hash)
            .map(|h| h as u32)
            .context("unknown block")
    }
    fn get_block_header(&self, hash: BlockHash) -> Result<bitcoin::block::Header> {
        Ok(self.get_block(hash)?.header)
    }
    fn get_block_count(&self) -> Result<u64> {
        Ok(self.0.read().unwrap().len() as u64 - 1)
    }
    fn get_raw_transaction_info(&self, _: &Txid) -> Result<TransactionInfo> {
        anyhow::bail!("txindex is unavailable")
    }
    fn get_mtp(&self, _: BlockHash) -> Result<u32> {
        Ok(0)
    }
}

pub fn transaction(input: OutPoint, tag: u8) -> Transaction {
    Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: input,
            script_sig: ScriptBuf::from_bytes(vec![1, tag]),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51, tag]),
        }],
    }
}

pub fn block(parent: &Block, tag: u8, transactions: Vec<Transaction>) -> Block {
    let mut block = Block {
        header: parent.header,
        txdata: vec![transaction(OutPoint::null(), tag)],
    };
    block.header.prev_blockhash = parent.block_hash();
    block.header.time += 1;
    block.header.nonce = u32::from(tag);
    block.txdata.extend(transactions);
    block.header.merkle_root = block.compute_merkle_root().unwrap();
    block
}

pub fn outpoint(tx: &Transaction) -> OutPoint {
    OutPoint {
        txid: tx.compute_txid(),
        vout: 0,
    }
}
