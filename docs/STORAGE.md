# Storage and recovery

## Block archive and proof retention

This describes Bitcoin mode. Shinigami mode writes its separate JSON proof/state exports.

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
limited-service advertisement. A rollback can also leave a temporary gap at the
old end of the window until processing fills a complete recent window again.

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

## Verification and small-fixture measurement

Run the supported Rust checks with `just ci`. For isolated real-node checks:

```sh
cargo build --locked
python3 contrib/regtest.py --utreexod /path/to/utreexod
```

The harness creates temporary regtest nodes and a temporary wallet. It checks
leaf, root, proof, and target parity against Utreexod; membership at an earlier
block after spending; disk pruning; restart after SIGKILL; and an equal-height
reorganization. Core runs with `txindex=0`. The reference flat proof index at
Utreexod `e99c32c` panics on `invalidateblock`, so the harness compares the bridge's
post-reorg state against a clean Utreexod replay of the replacement chain.

Pass `--consumer-project /path/to/limited-zk-bip-322` to run that client's real
REST membership and BIP324/BIP183 root checks before and after the spend. Its
fresh post-spend processes receive only lookup identifiers and node addresses.
This checks the node integration, not mainnet application ZK receipt acceptance.

A 127-block run using Core 30.2 and an unoptimized bridge took 2.168 seconds to
process its initial 102 mostly empty blocks. With retention set to 8, the final
proof files used 320 bytes; all eight retained blocks had no spent inputs. Total
bridge file sizes were 1,069,837 bytes, including checkpoints, journal, leaf index,
headers, roots, and the log. There was no bridge block archive. This small fixture
does not estimate mainnet disk use, peak memory, or initial-sync throughput.
