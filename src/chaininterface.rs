//SPDX License Identifier: MIT

//! A trait that abstracts how we pull data from the blockchain. We need some way to get blocks
//! and transactions from the blockchain, but we don't want to do consensus here. So we just
//! provide a trait that can be implemented by something, or at least talks to something
//! that does.

use anyhow::Result;
use bitcoin::block::Header;
use bitcoin::Block;
use bitcoin::BlockHash;
use bitcoin::Transaction;
use bitcoin::Txid;
use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::Client;
use bitcoincore_rpc::RpcApi;
use bitcoincore_rpc::{Auth, Error as RpcError};
use std::sync::{Arc, RwLock};

/// Core rotates cookie credentials on restart. Reload them before each call,
/// replacing the transport only when credentials change.
pub struct CoreRpc {
    url: String,
    auth: Auth,
    client: RwLock<(Auth, Arc<Client>)>,
}

impl CoreRpc {
    pub fn new(url: &str, auth: Auth) -> bitcoincore_rpc::Result<Self> {
        let url = if url.contains("://") {
            url.to_owned()
        } else {
            format!("http://{url}")
        };
        let url = url::Url::parse(&url)
            .map_err(|e| RpcError::ReturnedError(format!("invalid RPC URL: {e}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(RpcError::ReturnedError(
                "RPC URL must use HTTP or HTTPS".into(),
            ));
        }
        let url = url.to_string();
        let credentials = Self::credentials(&auth)?;
        let client = Arc::new(Self::transport(&url, credentials.clone())?);
        Ok(Self {
            url,
            auth,
            client: RwLock::new((credentials, client)),
        })
    }

    fn transport(url: &str, auth: Auth) -> bitcoincore_rpc::Result<Client> {
        if url.starts_with("https://") {
            // Client::new always uses plain TCP, even for an HTTPS URL.
            let (user, password) = auth.get_user_pass()?;
            let mut builder = jsonrpc::minreq_http::MinreqHttpTransport::builder()
                .url(url)
                .map_err(jsonrpc::Error::from)?;
            if let Some(user) = user {
                builder = builder.basic_auth(user, password);
            }
            Ok(Client::from_jsonrpc(jsonrpc::Client::with_transport(
                builder.build(),
            )))
        } else {
            Client::new(url, auth)
        }
    }

    fn credentials(auth: &Auth) -> bitcoincore_rpc::Result<Auth> {
        if matches!(auth, Auth::CookieFile(_)) {
            let (user, password) = auth.clone().get_user_pass()?;
            Ok(Auth::UserPass(
                user.ok_or(RpcError::InvalidCookieFile)?,
                password.ok_or(RpcError::InvalidCookieFile)?,
            ))
        } else {
            Ok(auth.clone())
        }
    }
}

impl RpcApi for CoreRpc {
    fn call<T: for<'a> serde::Deserialize<'a>>(
        &self,
        command: &str,
        args: &[serde_json::Value],
    ) -> bitcoincore_rpc::Result<T> {
        let credentials = Self::credentials(&self.auth)?;
        let client = {
            let current = self
                .client
                .read()
                .map_err(|_| RpcError::ReturnedError("RPC client lock poisoned".into()))?;
            if current.0 == credentials {
                current.1.clone()
            } else {
                drop(current);
                let mut current = self
                    .client
                    .write()
                    .map_err(|_| RpcError::ReturnedError("RPC client lock poisoned".into()))?;
                if current.0 != credentials {
                    *current = (
                        credentials.clone(),
                        Arc::new(Self::transport(&self.url, credentials)?),
                    );
                }
                current.1.clone()
            }
        };
        client.call(command, args)
    }
}

pub trait Blockchain: Send + Sync {
    /// Returns the entire content of a block, given a block hash.
    fn get_block(&self, block_hash: BlockHash) -> Result<Block>;
    /// Returns the entire content of a transaction, given a transaction id.
    fn get_transaction(&self, txid: Txid) -> Result<Transaction>;
    /// Returns the block hash of the block at the given height.
    fn get_block_hash(&self, height: u64) -> Result<BlockHash>;
    /// Returns the height of the block with the given hash.
    fn get_block_height(&self, block_hash: BlockHash) -> Result<u32>;
    /// Returns the block header of the block with the given hash.
    fn get_block_header(&self, block_hash: BlockHash) -> Result<Header>;
    /// Returns how many blocks are in the blockchain.
    fn get_block_count(&self) -> Result<u64>;
    /// Returns the raw transaction info, given a transaction id.
    fn get_raw_transaction_info(&self, txid: &Txid) -> Result<TransactionInfo>;
}

impl Blockchain for CoreRpc {
    fn get_block(&self, block_hash: BlockHash) -> Result<Block> {
        Ok(<Self as RpcApi>::get_block(self, &block_hash)?)
    }

    fn get_transaction(&self, txid: Txid) -> Result<Transaction> {
        Ok(self.get_raw_transaction(&txid, None)?)
    }

    fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        Ok(<Self as RpcApi>::get_block_hash(self, height)?)
    }

    fn get_block_height(&self, block_hash: BlockHash) -> Result<u32> {
        let info = self.get_block_info(&block_hash)?;
        Ok(info.height as u32)
    }

    fn get_block_header(&self, block_hash: BlockHash) -> Result<Header> {
        Ok(<Self as RpcApi>::get_block_header(self, &block_hash)?)
    }

    fn get_block_count(&self) -> Result<u64> {
        Ok(<Self as RpcApi>::get_block_count(self)?)
    }

    fn get_raw_transaction_info(&self, txid: &Txid) -> Result<TransactionInfo> {
        let tx_data = <Self as RpcApi>::get_raw_transaction_info(self, txid, None)?;
        let height = self.get_block_height(tx_data.blockhash.unwrap()).unwrap();
        Ok(TransactionInfo {
            tx: tx_data.transaction().unwrap(),
            height,
            blockhash: tx_data.blockhash,
            is_coinbase: tx_data.is_coinbase(),
        })
    }
}
#[derive(Debug)]
pub struct TransactionInfo {
    pub tx: Transaction,
    pub height: u32,
    pub blockhash: Option<BlockHash>,
    pub is_coinbase: bool,
}

impl<T: Blockchain + Sized> Blockchain for Box<T> {
    fn get_block(&self, block_hash: BlockHash) -> Result<Block> {
        (**self).get_block(block_hash)
    }

    fn get_transaction(&self, txid: Txid) -> Result<Transaction> {
        (**self).get_transaction(txid)
    }

    fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        (**self).get_block_hash(height)
    }

    fn get_block_height(&self, block_hash: BlockHash) -> Result<u32> {
        (**self).get_block_height(block_hash)
    }

    fn get_block_header(&self, block_hash: BlockHash) -> Result<Header> {
        (**self).get_block_header(block_hash)
    }

    fn get_block_count(&self) -> Result<u64> {
        (**self).get_block_count()
    }

    fn get_raw_transaction_info(&self, txid: &Txid) -> Result<TransactionInfo> {
        (**self).get_raw_transaction_info(txid)
    }
}

impl<T: Blockchain> Blockchain for &Box<T> {
    fn get_block(&self, block_hash: BlockHash) -> Result<Block> {
        (**self).get_block(block_hash)
    }

    fn get_transaction(&self, txid: Txid) -> Result<Transaction> {
        (**self).get_transaction(txid)
    }

    fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        (**self).get_block_hash(height)
    }

    fn get_block_height(&self, block_hash: BlockHash) -> Result<u32> {
        (**self).get_block_height(block_hash)
    }

    fn get_block_header(&self, block_hash: BlockHash) -> Result<Header> {
        (**self).get_block_header(block_hash)
    }

    fn get_block_count(&self) -> Result<u64> {
        (**self).get_block_count()
    }

    fn get_raw_transaction_info(&self, txid: &Txid) -> Result<TransactionInfo> {
        (**self).get_raw_transaction_info(txid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    fn https_rpc_never_falls_back_to_plaintext() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut header = [0u8; 5];
            socket.read_exact(&mut header).unwrap();
            header
        });
        let rpc = CoreRpc::new(
            &format!("HTTPS://{address}"),
            Auth::UserPass("test".into(), "secret".into()),
        )
        .unwrap();
        assert!(RpcApi::get_block_count(&rpc).is_err());
        let header = server.join().unwrap();
        assert_eq!(header[0], 0x16); // TLS handshake record, not an HTTP request.
        assert_eq!(header[1], 0x03);
    }
}
