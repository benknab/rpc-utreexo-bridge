"""Isolated Core/Utreexod/bridge integration; no existing nodes or wallets used.

Build bridge first. Pass --utreexod to compare an independent Go implementation.
This exercises bridge evidence retrieval, not a mainnet application ZK receipt.
"""

import argparse
import base64
import hashlib
import json
import os
import signal
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def rpc(url, method, *params):
    body = json.dumps(
        {"jsonrpc": "1.0", "id": 1, "method": method, "params": params}
    ).encode()
    request = urllib.request.Request(
        url,
        body,
        {
            "Content-Type": "application/json",
            "Authorization": "Basic "
            + base64.b64encode(b"bridge-test:bridge-test").decode(),
        },
    )
    try:
        response = urllib.request.urlopen(request, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        result = json.load(response)
    if result.get("error"):
        raise RuntimeError(f"{method}: {result['error']}")
    return result["result"]


def get(url, status=200):
    try:
        response = urllib.request.urlopen(url, timeout=35)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        assert response.status == status, (url, response.status, response.read())
        return json.load(response)["data"]


def wait(operation, label):
    deadline = time.monotonic() + 60
    last = None
    while time.monotonic() < deadline:
        try:
            result = operation()
            if result:
                return result
        except (OSError, RuntimeError, AssertionError) as error:
            last = error
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for {label}: {last}")


def sha(data):
    return hashlib.new("sha512_256", data).digest()


def verify(leaf, proof, state):
    """Verify a single fixed-63-row membership against freshly retrieved roots."""
    txid, vout = leaf["prevout"].split(":")
    script = bytes.fromhex(leaf["utxo"]["script_pubkey"])
    assert len(script) < 253
    preimage = bytes.fromhex(leaf["block_hash"])[::-1] + bytes.fromhex(txid)[::-1]
    preimage += struct.pack(
        "<IIQ",
        int(vout),
        leaf["block_height"] * 2 + leaf["is_coinbase"],
        leaf["utxo"]["value"],
    )
    preimage += bytes([len(script)]) + script
    tag = hashlib.sha512(b"UtreexoV1").digest()
    value = sha(tag + tag + preimage)
    assert value.hex() == leaf["hash"]
    assert proof["target_rows"] == 63 and len(proof["targets"]) == 1
    target = proof["targets"][0]
    row = 0
    while target & (1 << (63 - row)):
        row += 1
    start = (1 << 64) - (1 << (64 - row))
    offset = target - start
    base = offset << row
    leaves = state["leaves"]
    first = 0
    root_index = 0
    for root_row in range(leaves.bit_length() - 1, -1, -1):
        if leaves & (1 << root_row) == 0:
            continue
        end = first + (1 << root_row)
        if first <= base and base + (1 << row) <= end:
            assert len(proof["hashes"]) == root_row - row
            for text in proof["hashes"]:
                sibling = bytes.fromhex(text)
                value = sha(sibling + value if offset & 1 else value + sibling)
                offset >>= 1
            assert value.hex() == state["roots"][root_index]
            return
        first = end
        root_index += 1
    raise AssertionError("target outside occupied forest")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", default="target/debug/bridge")
    parser.add_argument("--bitcoind", default="bitcoind")
    parser.add_argument("--utreexod")
    parser.add_argument(
        "--rest", action="store_true", help="fetch binary blocks and headers via Core REST"
    )
    parser.add_argument(
        "--consumer-project",
        help="limited-zk-bip-322 checkout; runs its live bridge component tests",
    )
    parser.add_argument("--temp-parent")
    parser.add_argument(
        "--measure", action="store_true", help="also check the sync measurement runner"
    )
    args = parser.parse_args()
    processes = []
    with tempfile.TemporaryDirectory(
        prefix="bridge-regtest-", dir=args.temp_parent
    ) as temporary:
        root = Path(temporary)

        def start(command, name, env=None):
            with open(root / (name + ".log"), "ab") as log:
                process = subprocess.Popen(command, stdout=log, stderr=log, env=env)
            processes.append(process)
            return process

        core_port, api_port, p2p_port, reference_port = port(), port(), port(), port()
        core = f"http://127.0.0.1:{core_port}"
        api = f"http://127.0.0.1:{api_port}"
        reference = f"http://127.0.0.1:{reference_port}" if args.utreexod else None
        (root / "core").mkdir()
        start(
            [
                args.bitcoind,
                f"-datadir={root / 'core'}",
                "-regtest",
                "-server",
                "-rest=1",
                "-listen=0",
                "-connect=0",
                "-dnsseed=0",
                "-txindex=0",
                "-fallbackfee=0.0002",
                f"-rpcport={core_port}",
                "-rpcuser=bridge-test",
                "-rpcpassword=bridge-test",
            ],
            "core",
        )
        try:
            wait(lambda: rpc(core, "getblockchaininfo"), "Core")

            def start_reference(name):
                process = start(
                    [
                        args.utreexod,
                        "--regtest",
                        f"--datadir={root / name}",
                        f"--logdir={root / (name + '-logs')}",
                        "--nolisten",
                        "--nodnsseed",
                        "--notls",
                        f"--rpclisten=127.0.0.1:{reference_port}",
                        "--rpcuser=bridge-test",
                        "--rpcpass=bridge-test",
                        "--flatutreexoproofindex",
                    ],
                    name,
                )
                wait(lambda: rpc(reference, "getblockchaininfo"), "Utreexod")
                return process

            if reference:
                reference_process = start_reference("reference")
            rpc(core, "createwallet", "bridge-test")
            wallet = core + "/wallet/bridge-test"
            mining = rpc(wallet, "getnewaddress")
            rpc(core, "generatetoaddress", 101, mining)
            address = rpc(wallet, "getnewaddress")
            txid = rpc(wallet, "sendtoaddress", address, 0.1)
            tip_hash = rpc(core, "generatetoaddress", 1, mining)[0]
            tx = rpc(core, "getrawtransaction", txid, True, tip_hash)
            vout = next(
                o["n"]
                for o in tx["vout"]
                if o["scriptPubKey"].get("address") == address
            )
            outpoint = f"{txid}:{vout}"

            def feed_reference(first, last):
                if reference:
                    for height in range(first, last + 1):
                        block_hash = rpc(core, "getblockhash", height)
                        assert (
                            rpc(
                                reference,
                                "submitblock",
                                rpc(core, "getblock", block_hash, 0),
                            )
                            is None
                        )

            feed_reference(1, 102)
            env = dict(
                os.environ,
                DATA_DIR=str(root / "bridge"),
                API_HOST=f"127.0.0.1:{api_port}",
                P2P_HOST="127.0.0.1",
                P2P_PORT=str(p2p_port),
                BITCOIN_CORE_RPC_URL=core,
                BITCOIN_CORE_RPC_USER="bridge-test",
                BITCOIN_CORE_RPC_PASSWORD="bridge-test",
            )
            env.pop("ESPLORA_URL", None)
            if args.rest:
                env["BITCOIN_CORE_REST_URL"] = core
            else:
                env.pop("BITCOIN_CORE_REST_URL", None)
            command = [
                str(Path(args.bridge).resolve()),
                "--network",
                "regtest",
                "--proof-retention",
                "8",
                "--acc-snapshot-every-n-blocks",
                "16",
            ]
            started = time.monotonic()
            bridge = start(command, "bridge", env)
            wait(
                lambda: get(api + "/tip")["block_hash"] == tip_hash,
                "initial bridge sync",
            )
            sync_seconds = time.monotonic() - started
            captured = get(api + f"/leaf/{outpoint}/proof")
            assert captured["state"]["height"] == 102
            verify(captured["leaf"], captured["proof"], get(api + f"/roots/{tip_hash}"))
            bridge.send_signal(signal.SIGTERM)
            assert bridge.wait(timeout=15) == 0
            bridge = start(command, "bridge", env)
            wait(lambda: get(api + "/acc") == captured["state"], "SIGTERM restart")

            def check_consumer(spent):
                if args.consumer_project:
                    consumer_env = dict(
                        os.environ,
                        BRIDGE_TEST_URL=api,
                        BRIDGE_TEST_HASH=tip_hash,
                        BRIDGE_TEST_OUTPOINT=outpoint,
                        BRIDGE_TEST_SPENT=str(int(spent)),
                        BRIDGE_TEST_P2P_PORT=str(p2p_port),
                    )
                    subprocess.run(
                        [
                            "cargo",
                            "test",
                            "--locked",
                            "--release",
                            "-p",
                            "limited-bip322-host",
                            "--lib",
                            "live_bridge",
                            "--",
                            "--ignored",
                        ],
                        cwd=args.consumer_project,
                        env=consumer_env,
                        check=True,
                        timeout=300,
                    )

            check_consumer(False)
            if reference:
                state = rpc(reference, "getutreexoroots", tip_hash)
                assert captured["state"]["leaves"] == state["numleaves"]
                assert captured["state"]["roots"] == state["roots"]
                proof = rpc(reference, "proveutxochaintipinclusion", [txid], [vout], 1)
                assert captured["proof"]["targets"] == proof["prooftargets"]
                assert captured["proof"]["hashes"] == [
                    bytes.fromhex(h)[::-1].hex() for h in proof["proofhashes"]
                ]
                assert (
                    captured["leaf"]["hash"]
                    == bytes.fromhex(proof["hashesproven"][0])[::-1].hex()
                )

            # Spend the selected coin, advance beyond proof retention, then ask
            # for its original public state through a new HTTP request.
            raw = rpc(
                core,
                "createrawtransaction",
                [{"txid": txid, "vout": vout}],
                [{mining: 0.09998}],
            )
            signed = rpc(wallet, "signrawtransactionwithwallet", raw)
            assert signed["complete"]
            rpc(core, "sendrawtransaction", signed["hex"])
            latest = rpc(core, "generatetoaddress", 25, mining)[-1]
            feed_reference(103, 127)
            wait(lambda: get(api + "/tip")["block_hash"] == latest, "post-spend sync")
            get(api + f"/leaf/{outpoint}/proof", 404)
            get(api + "/block/102", 410)
            get(api + "/block/127")
            # Remove the captured roots: the verifier uses newly fetched state.
            captured.pop("state")
            fresh = get(api + f"/roots/{tip_hash}")
            header = bytes.fromhex(get(api + f"/header/{tip_hash}")["hex"])
            assert (
                hashlib.sha256(hashlib.sha256(header).digest()).digest()[::-1].hex()
                == tip_hash
            )
            assert fresh["block_hash"] == tip_hash and fresh["height"] == 102
            verify(captured["leaf"], captured["proof"], fresh)
            if reference:
                assert (
                    fresh["roots"]
                    == rpc(reference, "getutreexoroots", tip_hash)["roots"]
                )
            check_consumer(True)

            # Unclean exit between scheduled checkpoints, then an equal-height fork.
            before = get(api + "/acc")
            bridge.kill()
            bridge.wait(timeout=10)
            bridge = start(command, "bridge", env)
            wait(lambda: get(api + "/acc") == before, "journal replay after SIGKILL")
            rpc(core, "invalidateblock", latest)
            replacement = rpc(
                core, "generatetoaddress", 1, rpc(wallet, "getnewaddress")
            )[0]
            assert replacement != latest
            if reference:
                # Compare the bridge's rollback with an independent clean replay.
                # This also avoids the reference flat index's invalidateblock bug.
                reference_process.send_signal(signal.SIGINT)
                reference_process.wait(timeout=15)
                reference_process = start_reference("reference-replay")
                feed_reference(1, 127)
            wait(
                lambda: get(api + "/tip")["block_hash"] == replacement,
                "equal-height reorg",
            )
            get(api + f"/roots/{latest}", 404)
            if reference:
                assert (
                    get(api + "/acc")["roots"]
                    == rpc(reference, "getutreexoroots", replacement)["roots"]
                )
            files = list((root / "bridge" / "proofs").glob("*.proof"))
            assert len(files) == 8
            assert not (root / "bridge" / "blocks").exists()
            if args.measure:
                measure = [
                    "python3",
                    str(Path(__file__).with_name("measure.py")),
                    "--bridge",
                    str(Path(args.bridge).resolve()),
                    "--network",
                    "regtest",
                    "--proof-retention",
                    "8",
                    "--checkpoint-interval",
                    "16",
                    "--sample-seconds",
                    "0.1",
                ]
                subprocess.run(
                    [*measure, str(root / "measured"), "--stop-height", "120"],
                    env=env,
                    check=True,
                    timeout=60,
                    capture_output=True,
                )
                summary = json.loads((root / "measured/summary.json").read_text())
                assert summary["success"] and summary["sync"]["blocks"] == 120
                assert summary["sync"]["last_height"] == 120
                assert summary["sync"]["stages"]["checkpoint"]["nonzero_blocks"] == 7
                assert summary["final_storage"]["proofs"]["files"] == 8
                assert summary["final_storage"]["checkpoints"]["files"] == 2
                assert summary["peak_rss_bytes"] > 0
                timed_out = subprocess.run(
                    [
                        *measure,
                        str(root / "timed-out"),
                        "--stop-height",
                        "128",
                        "--timeout",
                        "2",
                    ],
                    env=env,
                    check=False,
                    timeout=30,
                    capture_output=True,
                )
                assert timed_out.returncode == 1, timed_out.stderr
                summary = json.loads((root / "timed-out/summary.json").read_text())
                assert not summary["success"] and summary["stop_reason"] == "timeout"
                assert (
                    summary["returncode"] == 0
                )  # Graceful partial sync is not benchmark success.
            print(
                json.dumps(
                    {
                        "regtest_passed": True,
                        "reference_compared": bool(reference),
                        "blocks": 127,
                        "initial_sync_seconds": round(sync_seconds, 3),
                        "retained_proofs": len(files),
                        "consumer_checked": bool(args.consumer_project),
                        "measurement_checked": args.measure,
                        "proof_bytes": sum(f.stat().st_size for f in files),
                        "txindex": False,
                        "bridge_file_bytes": sum(
                            f.stat().st_size
                            for f in (root / "bridge").rglob("*")
                            if f.is_file()
                        ),
                        "checks": [
                            "leaf/roots/targets",
                            "post-spend historical verification",
                            "pruning",
                            "SIGTERM shutdown",
                            "SIGKILL recovery",
                            "equal-height reorg",
                        ],
                    }
                )
            )
        except BaseException:
            for log in root.glob("*.log"):
                print(f"{log.name}:\n{log.read_text(errors='replace')[-6000:]}")
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


if __name__ == "__main__":
    main()
