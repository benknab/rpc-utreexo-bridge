use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use anyhow::Context;
use bitcoin::constants::genesis_block;
use clap::Parser;
use futures::channel::mpsc::channel;
use log::info;
use log::warn;

use crate::api;
use crate::chainview;
use crate::cli::CliArgs;
use crate::get_chain_provider;
use crate::init_logger;
use crate::leaf_cache::DiskLeafStorage;
use crate::node;
use crate::node::WorkerContext;
use crate::proofstore::ProofStore;
use crate::prover;
use crate::subdir;

pub fn run_bridge() -> anyhow::Result<()> {
    let cli_options = CliArgs::parse();
    fs::DirBuilder::new()
        .recursive(true)
        .create(subdir(""))
        .unwrap();

    // Initialize the logger
    init_logger(
        Some(&subdir("debug.log")),
        simplelog::LevelFilter::Info,
        true,
    );

    // to keep track of the current chain state and speed up replying to headers requests
    // from peers.
    let store = kv::Store::new(kv::Config {
        path: subdir("chain_view").into(),
        temporary: false,
        use_compression: false,
        flush_every_ms: None,
        cache_capacity: None,
        segment_size: None,
    })
    .expect("Failed to open chainview database");

    // Chainview is a collection of metadata about the chain, like tip and block
    // indexes. It's stored in a key-value database.
    let view = chainview::ChainView::new(store);
    let view = Arc::new(view);

    let network = cli_options.network;
    let genesis = genesis_block(network);

    view.initialize(genesis.header)?;

    anyhow::ensure!(
        !std::path::Path::new(&subdir("blocks")).exists(),
        "legacy combined block storage detected; use a fresh DATA_DIR"
    );
    let blocks = Arc::new(RwLock::new(ProofStore::new(
        subdir("proofs").into(),
        cli_options.proof_retention,
    )?));

    // The prover needs some way to pull blocks from a trusted source, we can use anything
    // implementing the [Blockchain] trait, for example a bitcoin core node or an esplora
    // instance.
    let client = get_chain_provider()?;
    anyhow::ensure!(
        client.get_block_hash(0)? == genesis.block_hash(),
        "backend network differs from --network"
    );

    // Process backend blocks and retain only derived state and recent proofs.
    let leaf_data = DiskLeafStorage::new(view.clone());

    // a signal used to stop the prover
    let kill_signal = Arc::new(Mutex::new(false));
    let stop = kill_signal.clone();
    ctrlc::set_handler(move || {
        warn!("Received a stop signal");
        *stop.lock().unwrap() = true;
    })?;

    //let leaf_data = HashMap::new(); // In-memory leaf storage,
    // faster than leaf_data but uses more memory

    let (block_notifier_tx, block_notifier_rx) = std::sync::mpsc::channel();
    let mut prover = prover::Prover::new(
        client,
        blocks.clone(),
        view.clone(),
        leaf_data,
        cli_options.initial_state_path.map(Into::into),
        cli_options.start_height,
        cli_options.acc_snapshot_every_n_blocks,
        kill_signal.clone(),
        cli_options.save_proofs_after.unwrap_or(0),
        block_notifier_tx,
    )?;

    info!("Starting P2PV2-only BIP 183 proof server");

    // This is our implementation of the Bitcoin p2p protocol, it will listen
    // for incoming connections and serve blocks and proofs to peers.
    let p2p_port = env::var("P2P_PORT").unwrap_or_else(|_| "8333".into());
    let p2p_address = format!(
        "{}:{}",
        env::var("P2P_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
        p2p_port
    );

    let worker_context = WorkerContext {
        chainview: view.clone(),
        magic: cli_options.network.magic(),
        proof_backend: blocks.clone(),
        block_backend: Arc::from(get_chain_provider()?),
    };

    let p2p_address = p2p_address.parse().context("invalid P2P listen address")?;
    node::Node::run(p2p_address, worker_context, block_notifier_rx)?;

    let (sender, receiver) = channel(1024);
    // This is our implementation of the REST API, it will listen for
    // incoming connections and serve some Utreexo data to clients.
    info!("Starting api");
    let host = env::var("API_HOST").unwrap_or_else(|_| "127.0.0.1:3000".into());
    std::thread::spawn(move || {
        actix_rt::System::new()
            .block_on(api::create_api(sender, view, &host))
            .unwrap()
    });

    // Keep the prover running in the background, it will download blocks and
    // create proofs for them as they are mined.
    info!("Running prover");
    prover.keep_up(cli_options.stop_height, receiver)
}
