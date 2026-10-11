# Core and bridge with Docker Compose

The included stack starts Bitcoin Core 30.2 and the bridge on signet. Core owns
the block archive. The bridge keeps its leaf index, roots, checkpoints, and
recent block proofs in a separate named volume. It mounts Core's volume read-only
to read the RPC cookie; it fetches blocks over RPC.

```sh
docker compose up --build --detach --wait
curl http://127.0.0.1:8080/tip
docker compose logs --follow bridge
```

Core's health check waits for RPC. The bridge's health check confirms its REST
listener; it does not mean the bridge has caught up. `/tip` reports processed
height and hash. A full sync starts from genesis.

## Configuration

Put Compose settings in `.env`, or export them before running Compose:

| Variable | Default | Use |
| --- | --- | --- |
| `BITCOIN_CHAIN` | `signet` | Core and bridge chain: `main`, `test`, `testnet4`, `signet`, or `regtest` |
| `API_PORT` | `8080` | Host REST port, bound to loopback |
| `P2P_PORT` | `38333` | Host P2PV2 port |
| `PROOF_RETENTION` | `288` | Number of recent block proofs |
| `CHECKPOINT_INTERVAL` | `1000` | Blocks between forest checkpoints |
| `BRIDGE_IMAGE` | `rpc-utreexo-bridge:local` | Locally built image tag |

Core RPC stays inside the Compose network. The stack uses its rotating cookie,
so no RPC username/password is needed. The bridge reloads that cookie after a
Core restart. Sync and outpoint proofs work with Core's transaction index off.

For mainnet, use a new Compose project so chain data cannot mix with signet:

```sh
BITCOIN_CHAIN=main P2P_PORT=8333 docker compose --project-name bridge-mainnet \
  up --build --detach --wait
```

Use the same chain settings and project name for later commands. Core needs disk
space for its archive, and the bridge needs space for its derived data. See
[storage and measurement](STORAGE.md) for what is retained and how to measure it.

Both volumes survive `docker compose down`. `down --volumes` deletes them and
requires a full rebuild. For upgrades from the old bind-mounted stack, keep the
old directories and start this stack with fresh volumes as described in the
[upgrade guide](STORAGE.md#upgrade-from-legacy-storage).

`docker compose stop bridge` sends SIGTERM. The bridge completes its current
block, flushes committed state, and exits. Restart restores its last checkpoint
and journal. Core outages leave already-processed roots and witnesses available.

## Existing Core backend

An existing archival Core node can supply blocks to a standalone bridge image:

```sh
docker build --tag rpc-utreexo-bridge:local .
docker run --name rpc-utreexo-bridge \
  --mount type=volume,src=rpc-utreexo-bridge-data,dst=/app/data \
  --env BITCOIN_CORE_RPC_URL=http://YOUR_CORE:8332 \
  --env BITCOIN_CORE_RPC_USER --env BITCOIN_CORE_RPC_PASSWORD \
  --publish 127.0.0.1:8080:3000 --publish 8333:8333 \
  rpc-utreexo-bridge:local --network bitcoin
```

Export both credentials before running that command, or mount a cookie read-only
and set `BITCOIN_CORE_COOKIE_FILE`. The image runs as UID/GID 101; bind-mounted
state and cookie files must be accessible to that user. HTTPS RPC endpoints use
certificate verification. The native bridge accepts the same RPC environment
variables; see [the sample configuration](../.env.sample).

## Verify the stack

```sh
python3 -B contrib/docker_regtest.py
```

The test creates a separate, uniquely named regtest stack. It checks proofs,
Core restart and cookie rotation, SIGTERM shutdown, and persistent recovery.
It removes its own test containers and volumes afterward.
