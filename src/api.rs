//SPDX-License-Identifier: MIT

//! This is a simple REST API that can be used to query Utreexo data. You can get the roots
//! of the accumulator, get a proof for a leaf, and get a block and the associated UData.
use std::str::FromStr;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::web;
use actix_web::App;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use actix_web::Responder;
use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::Amount;
use bitcoin::Block;
use bitcoin::BlockHash;
use bitcoin::OutPoint;
use bitcoin::TxOut;
use bitcoin::Txid;
use bitcoincore_rpc::jsonrpc::serde_json::json;
use futures::channel::mpsc::Sender;
use futures::lock::Mutex;
use futures::SinkExt;
use rustreexo::accumulator::node_hash::BitcoinNodeHash;
use rustreexo::accumulator::proof::Proof;
use serde::Deserialize;
use serde::Serialize;

use crate::chainview::ChainView;
use crate::prover::Requests;
use crate::prover::Responses;
use crate::state::{hash_hex, target_to_wire};
use crate::udata::CompactLeafData;
use crate::udata::LeafData;
use crate::udata::UtreexoBlock;

type SenderCh = Mutex<
    Sender<(
        Requests,
        futures::channel::oneshot::Sender<Result<Responses, String>>,
    )>,
>;

/// This is the state of the actix-web server that will be passed as reference by each
/// callback function. It contains a sender that can be used to send requests to the prover.
struct AppState {
    /// Sender to send requests to the prover.
    sender: SenderCh,
    view: Arc<ChainView>,
}

/// This function is used to send a request to the prover and wait for the response, and
/// return the response or an error.
async fn perform_request(
    data: &web::Data<AppState>,
    request: Requests,
) -> Result<Responses, String> {
    let (sender, receiver) = futures::channel::oneshot::channel();
    actix_web::rt::time::timeout(std::time::Duration::from_secs(30), async {
        data.sender
            .lock()
            .await
            .send((request, sender))
            .await
            .map_err(|_| "prover unavailable".to_owned())?;
        receiver
            .await
            .map_err(|_| "prover unavailable".to_owned())?
    })
    .await
    .map_err(|_| "prover request timed out".to_owned())?
}

/// the handler for the /transaction/{hash}/unpent endpoint. It returns the unspent outputs of a transaction given its hash, as well as the proof for those outpouts.
async fn get_tx_unspent(hash: web::Path<Txid>, data: web::Data<AppState>) -> impl Responder {
    let hash = hash.into_inner();
    let res = perform_request(&data, Requests::GetTxUnpent(hash)).await;
    match res {
        Ok(Responses::TransactionOut(outputs, proof, state)) => HttpResponse::Ok().json(json!({
            "error": null,
            "data": {
                "outputs": outputs,
                "proof": JsonProof::from(proof),
                "state":state,
            },
        })),
        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response",
            "data": null
        })),
        Err(e) => HttpResponse::InternalServerError().json(json!({
            "error": e,
            "data": null
        })),
    }
}

async fn get_leaf_data(outpoint: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let outpoint = outpoint.into_inner();
    let outpoint = OutPoint::from_str(&outpoint);

    if let Err(e) = outpoint {
        return HttpResponse::BadRequest().body(format!("Invalid outpoint {e}"));
    }

    let res = perform_request(&data, Requests::GetLeaf(outpoint.unwrap())).await;
    match res {
        Ok(Responses::LeafData(leaf_data)) => {
            let leaf_hash = hash_hex(LeafData::get_leaf_hashes(&leaf_data));
            let leaf_data = LeafDataResponse {
                hash: leaf_hash.clone(),
                block_hash: leaf_data.block_hash,
                prevout: OutPoint {
                    txid: leaf_data.txid,
                    vout: leaf_data.vout,
                },
                utxo: TxOut {
                    value: Amount::from_sat(leaf_data.value),
                    script_pubkey: leaf_data.pk_script,
                },
                block_height: leaf_data.block_height,
                is_coinbase: leaf_data.is_coinbase,
            };

            HttpResponse::Ok().json(json!({
                "error": null,
                "data": json!({
                    "hash": leaf_hash,
                    "leaf_data": leaf_data
                }),
            }))
        }

        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response",
            "data": null
        })),

        Err(e) => HttpResponse::BadRequest().json(json!({
            "error": e,
            "data": null,
        })),
    }
}
/// The handler for the `/proof/{hash}` endpoint. It returns a proof for the given hash, if
/// it exists.
async fn get_proof(hash: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let hash = hash.into_inner();
    let hash = BitcoinNodeHash::from_str(&hash);

    if let Err(e) = hash {
        return HttpResponse::BadRequest().body(format!("Invalid hash {e}"));
    }

    let res = perform_request(&data, Requests::GetProof(hash.unwrap())).await;

    match res {
        Ok(Responses::Proof(proof, state)) => HttpResponse::Ok().json(json!({
            "error": null,
            "data": {"targets":proof.targets, "hashes":proof.hashes.into_iter().map(hash_hex).collect::<Vec<_>>(),
                "target_rows":crate::state::forest_rows(state.leaves), "state":state},
        })),
        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response",
            "data": null
        })),
        Err(e) => HttpResponse::InternalServerError().json(json!({
            "error": e,
            "data": null
        })),
    }
}
async fn get_transaction(hash: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let hash = hash.into_inner();
    let hash = Txid::from_str(&hash);
    if let Err(e) = hash {
        return HttpResponse::BadRequest().body(format!("Invalid hash {e}"));
    }
    let res = perform_request(&data, Requests::GetTransaction(hash.unwrap())).await;

    match res {
        Ok(Responses::Transaction((tx, proof), state)) => HttpResponse::Ok().json(json!({
            "error": null,
            "data": {
                "tx": tx,
                "proof": JsonProof::from(proof),
                "state":state,
            },
        })),
        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response",
            "data": null
        })),
        Err(e) => HttpResponse::InternalServerError().json(json!({
            "error": e,
            "data": null
        })),
    }
}

/// The handler for the `/block/{height}` endpoint. It returns the block at the given height.
async fn get_block_by_height(height: web::Path<u32>, data: web::Data<AppState>) -> impl Responder {
    let height = height.into_inner();
    let res = perform_request(&data, Requests::GetBlockByHeight(height)).await;
    match res {
        Ok(Responses::Block(block)) => {
            let block: UBlock = deserialize::<UtreexoBlock>(&block).unwrap().into();
            HttpResponse::Ok().json(json!({ "error": null, "data": block}))
        }
        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response from backend",
            "data": null
        })),
        Err(e) if e == "block proof is pruned or unavailable" => {
            failure(actix_web::http::StatusCode::GONE, e)
        }
        Err(e) => failure(actix_web::http::StatusCode::BAD_GATEWAY, e),
    }
}
// Returns n blocks starting from the given height
async fn get_n_blocks(height: web::Path<(u32, u32)>, data: web::Data<AppState>) -> impl Responder {
    let (height, n) = height.into_inner();
    let res = perform_request(&data, Requests::GetBlocksByHeight(height, n)).await;
    match res {
        Ok(Responses::Blocks(blocks)) => {
            let blocks: Vec<UBlock> = blocks
                .into_iter()
                .map(|block| deserialize::<UtreexoBlock>(&block).unwrap().into())
                .collect();
            HttpResponse::Ok().json(json!({ "error": null, "data": blocks}))
        }
        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response from backend",
            "data": null
        })),
        Err(e) if e == "block proof is pruned or unavailable" => {
            failure(actix_web::http::StatusCode::GONE, e)
        }
        Err(e) => failure(actix_web::http::StatusCode::BAD_GATEWAY, e),
    }
}
/// Same as `get_roots`, but returns the leaf number of the accumulator too.
async fn get_roots_with_leaf(data: web::Data<AppState>) -> Result<HttpResponse, actix_web::Error> {
    let res = perform_request(&data, Requests::GetCSN).await;
    match res {
        Ok(Responses::Csn(acc)) => Ok(HttpResponse::Ok().json(json!({
            "error": null,
            "data": acc
        }))),
        Ok(_) => Ok(HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response",
            "data": null
        }))),
        Err(e) => Ok(HttpResponse::NotAcceptable().json(json!({
            "error": e,
            "data": null
        }))),
    }
}
/// The handler for the `/roots` endpoint. It returns the roots of the accumulator.
async fn get_roots(data: web::Data<AppState>) -> HttpResponse {
    let res = perform_request(&data, Requests::GetRoots).await;
    match res {
        Ok(Responses::Roots(roots)) => {
            let roots = roots.into_iter().map(hash_hex).collect::<Vec<String>>();

            HttpResponse::Ok().json(json!({
                "error": null,
                "data": roots
            }))
        }
        Ok(_) => HttpResponse::InternalServerError().json(json!({
            "error": "Invalid response",
            "data": null
        })),
        Err(e) => HttpResponse::NotAcceptable().json(json!({
            "error": e,
            "data": null
        })),
    }
}

async fn get_roots_for_block(
    hash: web::Path<BlockHash>,
    data: web::Data<AppState>,
) -> HttpResponse {
    let hash = hash.into_inner();
    let _guard = match data.view.gate.read() {
        Ok(guard) => guard,
        Err(_) => {
            return failure(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "chain lock poisoned",
            )
        }
    };
    match data.view.state(hash) {
        Ok(Some(acc)) => HttpResponse::Ok().json(json!({
            "error": null,
            "data": acc
        })),
        Ok(None) => HttpResponse::NotFound().json(json!({
            "error": "No roots found for this block",
            "data": null
        })),
        Err(e) => HttpResponse::InternalServerError().json(json!({
            "error": e.to_string(),
            "data": null
        })),
    }
}

fn failure(status: actix_web::http::StatusCode, error: impl ToString) -> HttpResponse {
    HttpResponse::build(status).json(json!({"error":error.to_string(),"data":null}))
}

async fn get_tip(data: web::Data<AppState>) -> HttpResponse {
    match data.view.tip() {
        Ok(Some(tip)) => HttpResponse::Ok().json(json!({"error":null,"data":tip})),
        Ok(None) => failure(
            actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
            "no processed tip",
        ),
        Err(e) => failure(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn get_block_hash(height: web::Path<u32>, data: web::Data<AppState>) -> HttpResponse {
    match data.view.get_block_hash(height.into_inner()) {
        Ok(Some(hash)) => HttpResponse::Ok().json(json!({"error":null,"data":hash})),
        Ok(None) => failure(
            actix_web::http::StatusCode::NOT_FOUND,
            "height not processed",
        ),
        Err(e) => failure(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn get_header(hash: web::Path<BlockHash>, data: web::Data<AppState>) -> HttpResponse {
    let hash = hash.into_inner();
    let result = (|| -> anyhow::Result<_> {
        let _guard = data
            .view
            .gate
            .read()
            .map_err(|_| anyhow::anyhow!("chain lock poisoned"))?;
        let Some(tip) = data.view.active_tip_for(hash)? else {
            return Ok(None);
        };
        let header = data
            .view
            .get_block(hash)?
            .ok_or_else(|| anyhow::anyhow!("missing header"))?;
        Ok(Some(
            json!({"height":tip.height, "block_hash":hash, "hex":hex::encode(header)}),
        ))
    })();
    match result {
        Ok(Some(header)) => HttpResponse::Ok().json(json!({"error":null,"data":header})),
        Ok(None) => failure(
            actix_web::http::StatusCode::NOT_FOUND,
            "block is not on the processed active chain",
        ),
        Err(e) => failure(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

async fn get_witness(outpoint: web::Path<OutPoint>, data: web::Data<AppState>) -> HttpResponse {
    match perform_request(&data, Requests::GetWitness(outpoint.into_inner())).await {
        Ok(Responses::Witness(leaf, proof, state)) => {
            let targets = proof
                .targets
                .into_iter()
                .map(|target| target_to_wire(target, state.leaves))
                .collect::<anyhow::Result<Vec<_>>>();
            let targets = match targets {
                Ok(targets) => targets,
                Err(e) => return failure(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR, e),
            };
            let leaf = LeafDataResponse {
                hash: hash_hex(LeafData::get_leaf_hashes(&leaf)),
                block_hash: leaf.block_hash,
                prevout: OutPoint {
                    txid: leaf.txid,
                    vout: leaf.vout,
                },
                utxo: TxOut {
                    value: Amount::from_sat(leaf.value),
                    script_pubkey: leaf.pk_script,
                },
                block_height: leaf.block_height,
                is_coinbase: leaf.is_coinbase,
            };
            HttpResponse::Ok().json(json!({"error":null,"data":{
                "leaf":leaf, "proof":{"targets":targets,"hashes":proof.hashes.into_iter().map(hash_hex).collect::<Vec<_>>(),"target_rows":63},
                "state":state,
            }}))
        }
        Ok(_) => failure(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected prover response",
        ),
        Err(e) => {
            let status = match e.as_str() {
                "outpoint is not unspent at processed tip" => {
                    actix_web::http::StatusCode::NOT_FOUND
                }
                "prover unavailable" => actix_web::http::StatusCode::SERVICE_UNAVAILABLE,
                "prover request timed out" => actix_web::http::StatusCode::GATEWAY_TIMEOUT,
                _ => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            failure(status, e)
        }
    }
}

/// This function creates the actix-web server and returns a future that can be awaited.
pub async fn create_api(
    request: Sender<(
        Requests,
        futures::channel::oneshot::Sender<Result<Responses, String>>,
    )>,
    view: Arc<ChainView>,
    host: &str,
) -> std::io::Result<()> {
    let app_state = web::Data::new(AppState {
        sender: Mutex::new(request),
        view,
    });

    HttpServer::new(move || {
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            .app_data(app_state.clone())
            .configure(routes)
    })
    .bind(host)?
    .run()
    .await
}

fn routes(cfg: &mut web::ServiceConfig) {
    cfg.route("/tip", web::get().to(get_tip))
        .route("/block/{height}/hash", web::get().to(get_block_hash))
        .route("/header/{hash}", web::get().to(get_header))
        .route("/leaf/{outpoint}/proof", web::get().to(get_witness))
        .route("/leaf/{outpoint}", web::get().to(get_leaf_data))
        .route("/prove/{leaf}", web::get().to(get_proof))
        .route("/roots", web::get().to(get_roots))
        .route("/block/{height}", web::get().to(get_block_by_height))
        .route("/tx/{hash}/outputs", web::get().to(get_transaction))
        .route("/acc", web::get().to(get_roots_with_leaf))
        .route("/batch_block/{height}/{n}", web::get().to(get_n_blocks))
        .route("/roots/{hash}", web::get().to(get_roots_for_block))
        .route("/tx/{hash}/unspent", web::get().to(get_tx_unspent));
}
/// The proof serialization by serde-json is not very nice, because it serializes byte-arrays
/// as a array of integers. This struct is used to serialize the proof in a nicer way.
#[derive(Clone, Serialize, Deserialize)]
struct JsonProof {
    targets: Vec<u64>,
    hashes: Vec<String>,
}

impl From<Proof> for JsonProof {
    fn from(proof: Proof) -> Self {
        let targets = proof.targets;
        let mut hashes = Vec::new();
        for hash in proof.hashes {
            hashes.push(hash_hex(hash));
        }
        JsonProof { targets, hashes }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct UBlock {
    block: Block,
    proof: JsonProof,
    leaf_data: Vec<CompactLeafData>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum ScriptPubkeyType {
    /// An non-specified type, in this case the script is just copied over
    Other(Box<[u8]>),
    /// p2pkh
    PubKeyHash,
    /// p2wsh
    WitnessV0PubKeyHash,
    /// p2sh
    ScriptHash,
    /// p2wsh
    WitnessV0ScriptHash,
}

impl From<UtreexoBlock> for UBlock {
    fn from(block: UtreexoBlock) -> Self {
        let proof = block.udata.as_ref().unwrap().proof.clone();
        let proof = Proof {
            hashes: proof
                .hashes
                .iter()
                .map(|x| BitcoinNodeHash::from(x.to_raw_hash().to_byte_array()))
                .collect(),
            targets: proof.targets.iter().map(|x| x.0).collect(),
        }
        .into();

        let leaves = block.udata.clone().unwrap().leaves.clone();
        let block = block.block;

        Self {
            block,
            proof,
            leaf_data: leaves,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct LeafDataResponse {
    hash: String,
    block_hash: BlockHash,
    prevout: OutPoint,
    utxo: TxOut,
    block_height: u32,
    is_coinbase: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chainview::{Delta, Tip};
    use crate::test_support::{block, store, TestDir};
    use actix_web::test;

    #[actix_web::test]
    async fn historical_resources_decode_state_and_reject_disconnected_blocks() {
        let dir = TestDir::new();
        let view = Arc::new(ChainView::new(store(&dir.join("chain"))).unwrap());
        let genesis = bitcoin::constants::genesis_block(bitcoin::Network::Regtest);
        view.initialize(genesis.header).unwrap();
        let first = block(&genesis, 1, vec![]);
        let tip = Tip {
            height: 1,
            block_hash: first.block_hash(),
        };
        let mut roots = 3u64.to_le_bytes().to_vec();
        roots.extend([1; 32]);
        roots.extend([0; 32]);
        view.commit(
            &Delta {
                tip,
                parent: genesis.block_hash(),
                adds: vec![],
                dels: vec![],
                changes: vec![],
            },
            &first.header,
            &roots,
        )
        .unwrap();
        let (sender, _receiver) = futures::channel::mpsc::channel(1);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(AppState {
                    sender: Mutex::new(sender),
                    view: view.clone(),
                }))
                .configure(routes),
        )
        .await;
        let path = format!("/roots/{}", tip.block_hash);
        let response: serde_json::Value =
            test::call_and_read_body_json(&app, test::TestRequest::get().uri(&path).to_request())
                .await;
        assert_eq!(response["data"]["leaves"], 3);
        assert_eq!(response["data"]["height"], 1);
        assert_eq!(
            response["data"]["roots"],
            json!(["01".repeat(32), "00".repeat(32)])
        );
        let header: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/header/{}", tip.block_hash))
                .to_request(),
        )
        .await;
        assert_eq!(
            header["data"]["hex"],
            hex::encode(bitcoin::consensus::serialize(&first.header))
        );
        let hash: serde_json::Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/block/1/hash").to_request(),
        )
        .await;
        assert_eq!(hash["data"], tip.block_hash.to_string());
        view.undo_tip().unwrap();
        assert!(view.get_acc(tip.block_hash).unwrap().is_some());
        let response =
            test::call_service(&app, test::TestRequest::get().uri(&path).to_request()).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::NOT_FOUND);
    }
}
