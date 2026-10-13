#[cfg(feature = "memory-leaf-map")]
use std::collections::HashMap;
use std::fs;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use clap::Parser;
use log::info;
use log::warn;

use crate::chainview;
use crate::cli::CliArgs;
use crate::get_chain_provider;
use crate::init_logger;
use crate::leaf_cache::DiskLeafStorage;
use crate::prover;
use crate::shinigami_block_storage::JsonBlockFiles;
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

    // This database stores the blocks themselves, it's a collection of flat files
    // that are indexed by the index above. They are stored in the `blocks/` directory
    // and are serialized as bitcoin blocks, so we don't need to do any parsing
    // before sending to a peer.
    let blocks = Arc::new(RwLock::new(JsonBlockFiles::new(
        subdir("blocks/").into(),
        cli_options.block_files_granularity,
    )));

    // The prover needs some way to pull blocks from a trusted source, we can use anything
    // implementing the [Blockchain] trait, for example a bitcoin core node or an esplora
    // instance.
    let client = get_chain_provider()?;
    let genesis = bitcoin::constants::genesis_block(cli_options.network);
    anyhow::ensure!(
        client.get_block_hash(0)? == genesis.block_hash(),
        "backend network differs from --network"
    );
    view.initialize(genesis.header)?;

    // Create a prover, this module will download blocks from the bitcoin core
    // node and save them to disk. It will also create proofs for the blocks
    // and save them to disk.
    #[cfg(not(feature = "memory-leaf-map"))]
    let leaf_data = DiskLeafStorage::with_config(
        view.clone(),
        usize::try_from(u64::from(cli_options.leaf_cache_mib) * 1024 * 1024)?,
        cli_options.leaf_read_threads as usize,
    )?;

    #[cfg(feature = "memory-leaf-map")]
    let leaf_data = HashMap::new();

    let kill_signal = Arc::new(Mutex::new(false));
    let stop = kill_signal.clone();
    ctrlc::set_handler(move || {
        warn!("Received a stop signal");
        *stop.lock().unwrap() = true;
    })?;
    let (sender, recv) = channel();
    let mut prover = prover::Prover::new(
        client,
        blocks,
        view,
        leaf_data,
        cli_options.initial_state_path.map(Into::into),
        cli_options.start_height,
        cli_options.acc_snapshot_every_n_blocks,
        kill_signal.clone(),
        cli_options.save_proofs_after.unwrap_or(0),
        sender,
    )?;

    // Keep the prover running in the background, it will download blocks and
    // create proofs for them as they are mined.
    info!("Running prover");
    prover.keep_up(cli_options.stop_height)?;
    drop(recv); // Don't let the receiver drop early, this will cause trouble inside the prover

    Ok(())
}
