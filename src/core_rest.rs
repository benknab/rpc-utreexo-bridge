// SPDX-License-Identifier: MIT

//! Core owns block storage. Responses and the connection pool are memory-only.
use std::io::Read;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use bitcoin::block::Header;
use bitcoin::consensus::deserialize;
use bitcoin::{Block, BlockHash};
use reqwest::blocking::Client;
use url::Url;

pub struct CoreRest {
    base: Url,
    client: Client,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{consensus::serialize, constants::genesis_block, hashes::Hash, Network};
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    fn serve(path: String, body: Vec<u8>) -> (CoreRest, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = CoreRest::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let task = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut input = BufReader::new(&socket);
            let mut line = String::new();
            input.read_line(&mut line).unwrap();
            assert_eq!(line, format!("GET /{path} HTTP/1.1\r\n"));
            loop {
                line.clear();
                input.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            let _ = socket.write_all(&body);
        });
        (client, task)
    }

    #[test]
    fn binary_blocks_are_hash_checked_and_size_bounded() {
        let block = genesis_block(Network::Regtest);
        for (requested, body, valid) in [
            (block.block_hash(), serialize(&block), true),
            (BlockHash::all_zeros(), serialize(&block), false),
            (block.block_hash(), vec![0; 4_000_001], false),
        ] {
            let (client, task) = serve(format!("rest/block/{requested}.bin"), body);
            assert_eq!(client.get_block(requested).is_ok(), valid);
            task.join().unwrap();
        }
    }

    #[test]
    fn headers_reject_reorg_empty_disconnected_and_truncated_responses() {
        let first = genesis_block(Network::Regtest).header;
        let mut second = first;
        second.prev_blockhash = first.block_hash();
        let mut valid = serialize(&first);
        valid.extend(serialize(&second));
        let mut disconnected = serialize(&first);
        disconnected.extend(serialize(&first));
        for (body, ok) in [
            (valid, true),
            (vec![], false),
            (disconnected, false),
            (vec![0; 81], false),
            (serialize(&second), false),
        ] {
            let (client, task) = serve(
                format!("rest/headers/{}.bin?count=2", first.block_hash()),
                body,
            );
            let result = client.get_block_hashes(first.block_hash(), 2);
            assert_eq!(result.is_ok(), ok);
            if let Ok(hashes) = result {
                assert_eq!(hashes, vec![first.block_hash(), second.block_hash()]);
            }
            task.join().unwrap();
        }
    }
}

impl CoreRest {
    pub fn new(url: &str) -> Result<Self> {
        let mut base = Url::parse(url)?;
        ensure!(
            matches!(base.scheme(), "http" | "https"),
            "REST URL must use HTTP or HTTPS"
        );
        ensure!(
            base.query().is_none() && base.fragment().is_none(),
            "REST URL must not contain a query or fragment"
        );
        ensure!(
            base.username().is_empty() && base.password().is_none(),
            "Core REST does not use URL credentials"
        );
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(4)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { base, client })
    }

    fn bytes(&self, path: &str, limit: usize) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(self.base.join(path)?)
            .send()?
            .error_for_status()?;
        ensure!(
            response.status().is_success(),
            "unexpected REST status {}",
            response.status()
        );
        if let Some(size) = response.content_length() {
            ensure!(size <= limit as u64, "oversized Core REST response");
        }
        let mut bytes = Vec::new();
        response.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= limit, "oversized Core REST response");
        Ok(bytes)
    }

    pub fn get_block(&self, hash: BlockHash) -> Result<Block> {
        let bytes = self.bytes(&format!("rest/block/{hash}.bin"), 4_000_000)?;
        let block: Block = deserialize(&bytes).context("invalid Core REST block")?;
        ensure!(block.block_hash() == hash, "Core REST returned wrong block");
        Ok(block)
    }

    pub fn get_block_hashes(&self, first: BlockHash, count: u32) -> Result<Vec<BlockHash>> {
        ensure!(
            (1..=2000).contains(&count),
            "REST header count must be 1..=2000"
        );
        let bytes = self.bytes(
            &format!("rest/headers/{first}.bin?count={count}"),
            count as usize * 80,
        )?;
        ensure!(
            !bytes.is_empty(),
            "Core REST header no longer in active chain"
        );
        ensure!(bytes.len() % 80 == 0, "invalid REST headers length");
        let mut hashes = Vec::with_capacity(bytes.len() / 80);
        for raw in bytes.chunks_exact(80) {
            let header: Header = deserialize(raw)?;
            if let Some(previous) = hashes.last() {
                ensure!(
                    header.prev_blockhash == *previous,
                    "disconnected REST headers"
                );
            } else {
                ensure!(
                    header.block_hash() == first,
                    "Core REST returned wrong first header"
                );
            }
            hashes.push(header.block_hash());
        }
        Ok(hashes)
    }
}
