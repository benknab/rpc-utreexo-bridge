"""Check the actual Compose stack, cookie rotation, and container shutdown.

Creates a unique regtest project and deletes only that project's test volumes.
"""

import argparse
import json
import os
import subprocess
import uuid
from pathlib import Path

from regtest import get, port, verify, wait


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    project = "bridge-test-" + uuid.uuid4().hex[:12]
    repo = Path(__file__).resolve().parent.parent
    api_port = port()
    api = f"http://127.0.0.1:{api_port}"
    env = dict(
        os.environ,
        BITCOIN_CHAIN="regtest",
        API_PORT=str(api_port),
        P2P_PORT=str(port()),
        PROOF_RETENTION="8",
        CHECKPOINT_INTERVAL="16",
    )

    def compose(*args, capture=True, check=True, timeout=180):
        return subprocess.run(
            ["docker", "compose", "--project-name", project, *args],
            cwd=repo,
            env=env,
            capture_output=capture,
            text=True,
            check=check,
            timeout=timeout,
        )

    def rpc(*args):
        result = compose(
            "exec",
            "-T",
            "bitcoind",
            "bitcoin-cli",
            "-regtest",
            "-rpcport=8332",
            "-rpccookiefile=/home/bitcoin/.bitcoin/.bridge-cookie",
            *args,
        ).stdout.strip()
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return result

    def inspect(container):
        return json.loads(
            subprocess.run(
                ["docker", "inspect", container],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            ).stdout
        )[0]

    try:
        if not args.skip_build:
            compose("build", "bridge", capture=False, timeout=900)
        compose("up", "--detach", "--wait", "--wait-timeout", "120", capture=False)
        assert rpc("getblockchaininfo")["chain"] == "regtest"
        rpc("createwallet", "bridge-test")
        address = rpc("getnewaddress")
        tip_hash = rpc("generatetoaddress", "102", address)[-1]
        wait(lambda: get(api + "/tip")["block_hash"] == tip_hash, "container sync")
        coin = rpc("listunspent")[0]
        outpoint = f"{coin['txid']}:{coin['vout']}"
        witness = get(api + f"/leaf/{outpoint}/proof")
        verify(witness["leaf"], witness["proof"], get(api + f"/roots/{tip_hash}"))
        bridge_id = compose("ps", "--quiet", "bridge").stdout.strip()
        started = inspect(bridge_id)["State"]["StartedAt"]
        old_cookie = compose(
            "exec",
            "-T",
            "bitcoind",
            "sha256sum",
            "/home/bitcoin/.bitcoin/.bridge-cookie",
        ).stdout
        compose("stop", "--timeout", "30", "bitcoind")
        assert get(api + "/acc") == witness["state"]
        compose("up", "--detach", "--no-deps", "--wait", "bitcoind")
        new_cookie = compose(
            "exec",
            "-T",
            "bitcoind",
            "sha256sum",
            "/home/bitcoin/.bitcoin/.bridge-cookie",
        ).stdout
        assert new_cookie != old_cookie
        latest = rpc("generatetoaddress", "1", address)[0]
        wait(lambda: get(api + "/tip")["block_hash"] == latest, "rotated-cookie sync")
        assert inspect(bridge_id)["State"]["StartedAt"] == started
        state = get(api + "/acc")
        compose("stop", "--timeout", "30", "bridge")
        assert inspect(bridge_id)["State"]["ExitCode"] == 0
        compose("up", "--detach", "--no-deps", "--wait", "bridge")
        wait(lambda: get(api + "/acc") == state, "container checkpoint replay")
        compose("exec", "-T", "bridge", "test", "!", "-e", "/app/data/blocks")
        print(
            json.dumps(
                {
                    "docker_regtest_passed": True,
                    "height": 103,
                    "checks": [
                        "cookie authentication",
                        "membership",
                        "Core restart and cookie rotation",
                        "bridge stayed running",
                        "SIGTERM exit 0",
                        "persistent checkpoint replay",
                        "no duplicate block archive",
                    ],
                }
            )
        )
    except BaseException:
        compose("logs", "--tail", "60", capture=False, check=False)
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans", capture=False, check=False)


if __name__ == "__main__":
    main()
