# Storage and recovery

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
