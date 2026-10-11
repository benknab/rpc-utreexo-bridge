use bitcoin::network::Network;
use clap::Parser;

fn network(value: &str) -> Result<Network, bitcoin::network::ParseNetworkError> {
    value.parse().or_else(|_| Network::from_core_arg(value))
}

#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Legacy snapshot height (unsupported: rebuild in a fresh DATA_DIR).
    #[clap(long, requires("initial_state_path"))]
    pub start_height: Option<u32>,
    /// Legacy accumulator-only import (unsupported without a matching leaf index).
    #[clap(long, requires("start_height"))]
    pub initial_state_path: Option<String>,
    /// Checkpoint interval (default: 1000). Keeps two checkpoints plus recovery deltas.
    #[clap(long)]
    pub acc_snapshot_every_n_blocks: Option<u32>,

    /// Flush and exit once this processed height is reached (for bounded sync runs).
    #[clap(long)]
    pub stop_height: Option<u32>,

    /// In shinigami mode, we save blocks individually in a json file. We also place those json
    /// inside a directory that has a range of blocks (e.g. 0-1000). This parameter specifies the
    /// range of blocks that will be saved in each directory. The default value is 10_000.
    #[clap(long, short = 'g', default_value_t = 10_000)]
    pub block_files_granularity: u32,

    /// Save proofs only above this absolute height (also subject to --proof-retention).
    #[clap(long)]
    pub save_proofs_after: Option<u32>,

    /// Number of recent block proofs to retain (historical roots are always retained).
    #[clap(long, default_value_t = 288)]
    pub proof_retention: u32,

    /// Bitcoin network (also accepts Core's main/test chain names).
    #[clap(long, short = 'n', default_value = "bitcoin", value_parser = network)]
    pub network: Network,
}
