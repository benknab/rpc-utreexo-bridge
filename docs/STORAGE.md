# Storage and recovery

## Block archive and proof retention

Bitcoin Core (or the configured `Blockchain` backend) owns the Bitcoin block
archive. The bridge stores no block bodies. `proofs/` holds one atomic proof-only
record per retained height; REST combines those proofs with backend block data
on demand. P2P proof requests read only local proofs. P2P block requests go through
bounded background workers, so an RPC stall does not block pings or proofs.

`--proof-retention 288` is the default. Values must be positive. Proof files older
than that rolling window are removed from disk, and orphaned future records are
removed during recovery. Historical roots and leaf counts are retained for every
processed active-chain block regardless of proof retention. `--save-proofs-after`
is an additional **absolute height cutoff**, not a window size.

The bridge advertises `NODE_UTREEXO`, witness, and P2PV2. It adds
`NODE_NETWORK_LIMITED` when it has proofs for the latest 288 processed blocks
(or every block of a shorter chain). It does not advertise historical proof/archive
service. Windows below 288 and gaps caused by `--save-proofs-after` can prevent
limited-service advertisement.

The backend must retain the blocks clients request. An archival Core backend is
recommended. Sync and outpoint membership do not require `txindex`; legacy
transaction-by-ID endpoints use `getrawtransaction` and require that index.

## Processed state

The processed tip is identified by **height and block hash**. Leaves, headers,
historical accumulator states, and recovery deltas are published in one atomic
database batch per block. REST proof queries execute between those batches and
never consume leaf data.

`--acc-snapshot-every-n-blocks` defaults to 1000. The bridge keeps the two most
recent forest checkpoints and the replay/undo deltas after the older checkpoint.
It detects reorganizations by hash, including equal-height replacements. Recovery
coverage is checked before disconnecting blocks. A reorganization beyond retained
recovery data stops processing and requires a rebuild in a fresh data directory.
Historical roots are retained separately from this bounded recovery data.

On restart, the bridge loads a checkpoint and replays committed deltas, checking
the roots after every replayed block. A shutdown snapshot is not required. Forest
snapshots are atomically renamed only after the corresponding database state is
durable; journal pruning happens after checkpoint publication.

## Upgrade from legacy storage

Use a **fresh `DATA_DIR`** and rebuild from genesis. Preserve the previous directory
until the new instance has caught up. The old `pollard`, leaf cache, and saved
height were not committed together, so they cannot be safely imported as a complete
state. Accumulator-only `--initial-state-path`/`--start-height` imports are rejected
for the same reason. The bridge never deletes a legacy directory for you.

The backend network must match `--network` and the stored genesis hash.
