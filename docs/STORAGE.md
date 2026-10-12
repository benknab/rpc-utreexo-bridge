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

Sync fetches each block by hash. It computes the parent median time from the
eleven most recent stored headers, avoiding an extra full-block metadata RPC
request and giving Esplora-backed leaves the same median-time data.

Set `BITCOIN_CORE_REST_URL=http://bitcoind:8332` and enable Core `rest=1` to use
pooled binary block downloads instead of hex inside JSON-RPC. RPC credentials
are still used for control and transaction queries. REST is opt-in, fails
explicitly if unavailable, and does not require Core 31. The Compose stack
enables it by default. Core REST is unauthenticated; keep it on the private
backend network. Responses are size-bounded, hash-checked, and never cached on disk.

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
python3 contrib/regtest.py --rest --measure
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

A later optimized run on an M4 Pro with 24 GB RAM and local Core 30.2 processed
172 regtest blocks containing 224 transactions and 16,404 spent inputs. The
fixture kept 10,392 live outputs and used a 16-block checkpoint interval and
eight-block proof window. Sync took 5.283 seconds, with 123 MiB peak resident
memory and 66.7 MiB of final bridge files. Eight nonempty proofs used 68,864 bytes.
Proof writes took 1.659 seconds, database flushes 1.307 seconds, pruning 0.821
seconds, and ten checkpoints 0.293 seconds in total. Restart restored the forest
in 33.7 ms, and its roots matched an independent Utreexod replay. Core had just
generated the fixture; filesystem caches were not cleared. These are small,
warm-regtest measurements, not a mainnet capacity or throughput estimate.

## Repeatable sync measurements

Build an optimized binary and select the backend with the normal RPC environment
variables or `.env`. Then run on macOS or Linux with Python 3.11 or newer:

```sh
cargo build --locked --release
python3 contrib/measure.py /path/to/new-measurement \
  --stop-height 100000 --timeout 3600
```

The output directory must be new. The runner sets a fresh `DATA_DIR` inside it
and binds REST/P2P to local ports. It retains `data/`, `process.log`, resource
samples in `samples.jsonl`, and `summary.json`. Use `--network regtest` for a
regtest backend. `--proof-retention` and `--checkpoint-interval` control the
usual storage settings; their defaults remain 288 and 1000.

`--stop-height` is also a bridge option. It flushes and exits at that height,
even if the backend has advanced further. A later run can resume from the same
data directory. The measurement runner always starts from genesis. A timeout
keeps the partial measurements, exits nonzero, and marks the summary unsuccessful.

The summary includes the binary hash, command, platform, wall time, CPU time,
and the OS-reported peak resident memory of the bridge process. It reports final
file sizes and allocated disk bytes by storage directory. Sampled disk peaks are
estimates and can miss short-lived checkpoint files. Resident memory includes
mapped file pages, but excludes Core and the unmapped filesystem cache. Sampling
itself adds some overhead.

Each processed-block log includes microseconds spent fetching, preparing leaves,
proving, changing the forest, writing proofs, committing the database, flushing,
pruning, and checkpointing. `block_us` covers those stages together; do not add it
to the individual stage totals. Checkpoint time includes snapshot publication and
journal pruning. Separate log lines report checkpoint size and restore time.
`leaves` counts all accumulator insertions, including later spent leaves.

Record the Core version, backend location, CPU/RAM/disk, height range, and whether
the filesystem cache was warm alongside results. Compare identical block ranges
and settings. Small regtest or early-mainnet ranges do not predict a full sync.

`python3 contrib/regtest.py --measure` checks exact bounded sync, resource output,
and unsuccessful timeout reporting against an isolated real Core node.
