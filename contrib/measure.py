"""Measure a bounded bridge sync from genesis against an existing backend.

Uses the normal backend environment/.env settings. Creates a new directory for
bridge state, logs, resource samples, and a JSON summary. Requires macOS or Linux.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def storage(directory):
    groups = {}
    for path in directory.rglob("*"):
        try:
            if not path.is_file():
                continue
            stat = path.stat()
        except FileNotFoundError:
            continue  # A checkpoint or proof was pruned during the sample.
        parts = path.relative_to(directory).parts
        name = parts[0] if len(parts) > 1 else "other"
        group = groups.setdefault(name, {"files": 0, "bytes": 0, "allocated_bytes": 0})
        group["files"] += 1
        group["bytes"] += stat.st_size
        group["allocated_bytes"] += stat.st_blocks * 512
    return groups


def resident_bytes(pid):
    if sys.platform == "linux":
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        except FileNotFoundError:
            pass
        return None
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    return int(result.stdout.strip()) * 1024 if result.stdout.strip() else None


def send_signal(pid, sig):
    # Popen.send_signal/poll can reap the child before wait4 collects its usage.
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        pass


def log_metrics(path):
    result = {"blocks": 0, "transactions": 0, "stages": {}, "stop_height": None}
    with path.open() as log:
        for line in log:
            if "Reached stop height=" in line:
                result["stop_height"] = int(line.rsplit("=", 1)[1])
            if "processed height=" not in line:
                continue
            fields = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)}
            result["blocks"] += 1
            result["transactions"] += fields["txs"]
            result.setdefault("first_height", fields["height"])
            result["last_height"] = fields["height"]
            result["leaves"] = fields["leaves"]
            for key, value in fields.items():
                if not key.endswith("_us"):
                    continue
                stage = result["stages"].setdefault(
                    key[:-3], {"total_us": 0, "max_us": 0, "nonzero_blocks": 0}
                )
                stage["total_us"] += value
                stage["max_us"] = max(stage["max_us"], value)
                stage["nonzero_blocks"] += int(value > 0)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory", type=Path, help="new output directory; must not exist"
    )
    parser.add_argument("--bridge", type=Path, default=Path("target/release/bridge"))
    parser.add_argument("--network", default="bitcoin")
    parser.add_argument("--stop-height", type=int, required=True)
    parser.add_argument("--proof-retention", type=int, default=288)
    parser.add_argument("--checkpoint-interval", type=int, default=1000)
    parser.add_argument("--sample-seconds", type=float, default=5)
    parser.add_argument(
        "--timeout", type=float, default=3600, help="wall-clock seconds"
    )
    args = parser.parse_args()
    if sys.platform not in ("darwin", "linux"):
        parser.error("resource measurement requires macOS or Linux")
    if args.stop_height < 0 or min(args.proof_retention, args.checkpoint_interval) <= 0:
        parser.error(
            "height must be nonnegative; retention and interval must be positive"
        )
    if not 0 < args.sample_seconds < float("inf") or not 0 < args.timeout < float(
        "inf"
    ):
        parser.error("sampling interval and timeout must be finite positive seconds")
    binary = args.bridge.resolve(strict=True)
    with binary.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    output = args.directory.resolve()
    output.mkdir(parents=True)  # Refuse to overwrite measurements or reuse chain state.
    data = output / "data"
    data.mkdir()
    command = [
        str(binary),
        "--network",
        args.network,
        "--stop-height",
        str(args.stop_height),
        "--proof-retention",
        str(args.proof_retention),
        "--acc-snapshot-every-n-blocks",
        str(args.checkpoint_interval),
    ]
    env = dict(
        os.environ,
        DATA_DIR=str(data),
        API_HOST=f"127.0.0.1:{port()}",
        P2P_HOST="127.0.0.1",
        P2P_PORT=str(port()),
    )
    started = time.monotonic()
    stop_reason = None
    stop_sent = None
    next_sample = started
    peak_disk = {"bytes": 0, "allocated_bytes": 0}
    with (output / "process.log").open("w") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=log)
    try:
        with (output / "samples.jsonl").open("w") as samples:
            while True:
                try:
                    pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                    if pid:
                        process.returncode = os.waitstatus_to_exitcode(status)
                        break
                    now = time.monotonic()
                    if stop_sent is None and now - started >= args.timeout:
                        stop_reason = "timeout"
                        stop_sent = now
                        send_signal(process.pid, signal.SIGINT)
                    if stop_sent is not None and now - stop_sent >= 120:
                        send_signal(process.pid, signal.SIGKILL)
                    if now >= next_sample:
                        sizes = storage(data)
                        for key, previous in peak_disk.items():
                            peak_disk[key] = max(
                                previous,
                                sum(group[key] for group in sizes.values()),
                            )
                        sample = {
                            "elapsed_seconds": round(time.monotonic() - started, 6),
                            "rss_bytes": resident_bytes(process.pid),
                            "storage": sizes,
                        }
                        samples.write(json.dumps(sample) + "\n")
                        samples.flush()
                        next_sample = time.monotonic() + args.sample_seconds
                    time.sleep(min(max(0, next_sample - time.monotonic()), 0.1))
                except KeyboardInterrupt:
                    if stop_sent is None:
                        stop_reason = "interrupted"
                        stop_sent = time.monotonic()
                        send_signal(process.pid, signal.SIGINT)
    finally:
        if process.returncode is None:
            send_signal(process.pid, signal.SIGKILL)
            _, status, _ = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
    wall = time.monotonic() - started
    sizes = storage(data)
    for key, previous in peak_disk.items():
        peak_disk[key] = max(previous, sum(group[key] for group in sizes.values()))
    metrics = log_metrics(output / "process.log")
    success = process.returncode == 0 and metrics["stop_height"] == args.stop_height
    summary = {
        "success": success,
        "stop_reason": stop_reason or "exit",
        "returncode": process.returncode,
        "command": command,
        "platform": platform.platform(),
        "binary_sha256": digest,
        "wall_seconds": round(wall, 6),
        "user_cpu_seconds": usage.ru_utime,
        "system_cpu_seconds": usage.ru_stime,
        "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
        "sampled_peak_storage": peak_disk,
        "final_storage": sizes,
        "sync": metrics,
        "blocks_per_second": metrics["blocks"] / wall,
        "transactions_per_second": metrics["transactions"] / wall,
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
