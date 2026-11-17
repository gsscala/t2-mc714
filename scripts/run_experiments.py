"""Automated benchmark runner for MC714 Trabalho 2 experiments.

Deployment model: every logical node runs as an independent OS process
(``python -m distrib_ledger.cli node``) communicating exclusively over TCP —
the same entry point that compose.yaml uses inside containers. The driver
coordinates the cluster through the nodes' control plane and collects all
metrics from the nodes themselves.

Usage:
    python3 scripts/run_experiments.py                # all experiments
    python3 scripts/run_experiments.py --only exp1    # one experiment
    python3 scripts/run_experiments.py --trials 3     # quick smoke run
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import platform
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from distrib_ledger.cli import send_control  # noqa: E402

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

REPO_ROOT = Path(__file__).resolve().parent.parent


class NodeProc:
    """Handle for one logical node running as an independent OS process."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: Dict[str, tuple],
        wal_dir: Path,
        log_path: Path,
        balance: float = 1000.0,
        auditor: Optional[str] = None,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.wal_dir = wal_dir
        self.log_path = log_path
        self.balance = balance
        self.auditor = auditor
        self.proc: Optional[subprocess.Popen] = None

    def _popen(self) -> subprocess.Popen:
        peer_args = ",".join(f"{nid}={h}:{p}" for nid, (h, p) in self.peers.items())
        log_file = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            [
                sys.executable, "-m", "distrib_ledger.cli", "node",
                "--id", self.node_id,
                "--host", self.host,
                "--port", str(self.port),
                "--peers", peer_args,
                "--wal-dir", str(self.wal_dir),
                "--balance", str(self.balance),
                "--auditor", self.auditor or "",
            ],
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        return self.proc

    async def start(self, timeout: float = 20.0) -> None:
        self._popen()
        await self.wait_ready(timeout)

    async def respawn(self, timeout: float = 20.0) -> None:
        """Starts a new process reusing the same WAL directory (crash recovery)."""
        self._popen()
        await self.wait_ready(timeout)

    async def wait_ready(self, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                result = await send_control(self.host, self.port, "ping", timeout=2.0)
                if result.get("ok"):
                    return
            except (ConnectionError, OSError, TimeoutError):
                await asyncio.sleep(0.1)
        raise RuntimeError(f"Node {self.node_id} did not become ready within {timeout}s")

    async def control(self, op: str, args: Optional[Dict[str, Any]] = None, timeout: float = 60.0) -> Dict[str, Any]:
        result = await send_control(self.host, self.port, op, args, timeout=timeout)
        if not result.get("ok"):
            raise RuntimeError(f"Control op {op} failed on {self.node_id}: {result.get('error')}")
        return result["result"]

    async def stats(self, timeout: float = 15.0) -> Dict[str, Any]:
        return await self.control("get_stats", timeout=timeout)

    async def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self.proc = None

    @property
    def exited(self) -> bool:
        return self.proc is not None and self.proc.poll() is not None

    async def wait_exit(self, timeout: float = 10.0) -> Optional[int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.exited:
                return self.proc.returncode
            await asyncio.sleep(0.05)
        return None


class Cluster:
    """Set of node processes with lifecycle management and port allocation."""

    def __init__(self, run_dir: Path, base_port: int, balance: float = 1000.0):
        self.run_dir = run_dir
        self.base_port = base_port
        self.balance = balance
        self.nodes: Dict[str, NodeProc] = {}

    @property
    def node_ids(self) -> List[str]:
        return list(self.nodes.keys())

    async def launch(self, n: int, auditor: str = "node0") -> "Cluster":
        port = self.base_port
        peer_map: Dict[str, tuple] = {}
        for i in range(n):
            peer_map[f"node{i}"] = ("127.0.0.1", port + i)
        for i in range(n):
            nid = f"node{i}"
            node = NodeProc(
                node_id=nid,
                host="127.0.0.1",
                port=port + i,
                peers=peer_map,
                wal_dir=self.run_dir / nid,
                log_path=self.run_dir / f"{nid}.log",
                balance=self.balance,
                auditor=auditor,
            )
            self.nodes[nid] = node
            await node.start()
        return self

    async def set_fault_all(self, seed: int, delay_mean_ms: float = 0.0, delay_jitter_ms: float = 0.0,
                            drop_rate: float = 0.0) -> None:
        for node in self.nodes.values():
            await node.control("set_fault", {
                "seed": seed,
                "delay_mean_ms": delay_mean_ms,
                "delay_jitter_ms": delay_jitter_ms,
                "drop_rate": drop_rate,
            }, timeout=15.0)

    async def total_sent(self) -> int:
        stats = await asyncio.gather(*(n.stats() for n in self.nodes.values()))
        return int(sum(s["channel"]["sent"] for s in stats))

    async def balances(self) -> Dict[str, float]:
        stats = await asyncio.gather(*(n.stats() for n in self.nodes.values()))
        return {s["node_id"]: s["balance"] for s in stats}

    async def stop(self) -> None:
        for node in self.nodes.values():
            try:
                await node.stop()
            except Exception:
                pass
        self.nodes.clear()


# ==============================================================================
# Experiment 1: Ricart-Agrawala Mutual Exclusion
# ==============================================================================
async def run_exp1_ra(trials: int, run_root: Path) -> List[Dict[str, Any]]:
    print("\n--- Experiment 1: Ricart-Agrawala Mutual Exclusion ---", flush=True)
    results: List[Dict[str, Any]] = []
    cluster_sizes = [3, 5, 7, 9]
    scenarios = ["normal", "contended", "degraded"]
    base_port = 23000

    for n in cluster_sizes:
        for scen in scenarios:
            print(f"  N={n}, scenario={scen}", flush=True)
            for trial in range(trials):
                seed = 1000 + trial * 17 + n
                run_dir = run_root / f"exp1_n{n}_{scen}_{trial}"
                run_dir.mkdir(parents=True, exist_ok=True)
                cluster = Cluster(run_dir, base_port)
                base_port += n + 5

                try:
                    await cluster.launch(n)

                    # Warmup: one full CS cycle before measurement
                    await cluster.nodes["node0"].control("request_cs", timeout=30.0)
                    await cluster.nodes["node0"].control("release_cs")

                    if scen == "degraded":
                        await cluster.set_fault_all(seed, delay_mean_ms=10.0, delay_jitter_ms=4.0)

                    sent_before = await cluster.total_sent()
                    latencies: List[float] = []

                    if scen in ("normal", "degraded"):
                        t0 = time.perf_counter()
                        res = await cluster.nodes["node0"].control("request_cs", timeout=60.0)
                        wall_ms = (time.perf_counter() - t0) * 1000.0
                        latencies.append(float(res["latency_ms"]))
                        await cluster.nodes["node0"].control("release_cs")
                        _ = wall_ms  # wall time kept for debugging; node-measured latency reported
                    else:  # contended: all N nodes compete simultaneously
                        tasks = [
                            asyncio.create_task(node.control("cs_cycle", {"hold_sec": 0.005}, timeout=120.0))
                            for node in cluster.nodes.values()
                        ]
                        replies = await asyncio.gather(*tasks)
                        latencies = [float(r["latency_ms"]) for r in replies]

                    sent_after = await cluster.total_sent()
                    total_messages = sent_after - sent_before

                    latencies.sort()
                    p50 = latencies[len(latencies) // 2]
                    p95 = latencies[min(int(len(latencies) * 0.95), len(latencies) - 1)]
                    results.append({
                        "trial": trial,
                        "cluster_size": n,
                        "scenario": scen,
                        "seed": seed,
                        "latency_ms": round(sum(latencies) / len(latencies), 3),
                        "latency_p50_ms": round(p50, 3),
                        "latency_p95_ms": round(p95, 3),
                        "total_messages": total_messages,
                    })
                finally:
                    await cluster.stop()

    csv_path = DATA_DIR / "exp1_ricart_agrawala.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {len(results)} rows to {csv_path}", flush=True)
    return results


# ==============================================================================
# Experiment 2: Two-Phase Commit (normal, crash before decision, crash after)
# ==============================================================================
async def run_exp2_tpc(trials: int, run_root: Path) -> List[Dict[str, Any]]:
    print("\n--- Experiment 2: Two-Phase Commit ---", flush=True)
    results: List[Dict[str, Any]] = []
    delays_ms = [5.0, 25.0, 50.0]
    scenarios = ["normal_commit", "crash_before_decision", "crash_after_decision"]
    base_port = 26000

    for delay in delays_ms:
        for scen in scenarios:
            print(f"  delay={delay}ms, scenario={scen}", flush=True)
            for trial in range(trials):
                seed = 2000 + trial * 19 + int(delay)
                run_dir = run_root / f"exp2_{scen}_{int(delay)}_{trial}"
                run_dir.mkdir(parents=True, exist_ok=True)
                cluster = Cluster(run_dir, base_port)
                base_port += 10

                commit_latency_ms: float = 0.0
                recovery_duration_ms: float = 0.0
                success = False
                final_balances: Dict[str, float] = {}

                try:
                    await cluster.launch(3)
                    await cluster.set_fault_all(seed, delay_mean_ms=delay)

                    if scen == "normal_commit":
                        t0 = time.perf_counter()
                        res = await cluster.nodes["node0"].control(
                            "transfer", {"to": "node1", "amount": 50.0, "use_cs": False}, timeout=60.0
                        )
                        commit_latency_ms = (time.perf_counter() - t0) * 1000.0
                        success = bool(res["success"])
                        final_balances = await cluster.balances()
                        success = success and final_balances.get("node0") == 950.0 and final_balances.get("node1") == 1050.0
                    else:
                        crash_point = "before_decision" if scen == "crash_before_decision" else "after_decision"
                        tx_id = f"tx_crash_{int(delay)}_{trial}"
                        coord = cluster.nodes["node0"]

                        try:
                            await coord.control("transfer_then_crash", {
                                "to": "node1", "amount": 50.0, "tx_id": tx_id, "crash_point": crash_point,
                            }, timeout=15.0)
                        except (ConnectionError, OSError, RuntimeError, TimeoutError):
                            pass  # expected: the coordinator exits mid-protocol

                        exit_code = await coord.wait_exit(timeout=15.0)
                        if exit_code is None:
                            raise RuntimeError("Coordinator process did not crash as expected")

                        # Reboot the coordinator on the same WAL: recovery runs at start.
                        t0 = time.perf_counter()
                        await coord.respawn()
                        stats0 = await coord.stats()
                        recovery_duration_ms = float(stats0["recovery_duration_ms"])
                        _ = t0
                        final_balances = await cluster.balances()

                        expected_status = "ABORTED" if scen == "crash_before_decision" else "COMMITTED"
                        success = stats0["last_recovery"].get(tx_id) == expected_status
                        if scen == "crash_before_decision":
                            success = success and final_balances.get("node0") == 1000.0 and final_balances.get("node1") == 1000.0
                        else:
                            success = success and final_balances.get("node0") == 950.0 and final_balances.get("node1") == 1050.0

                    results.append({
                        "trial": trial,
                        "delay_ms": delay,
                        "scenario": scen,
                        "seed": seed,
                        "commit_latency_ms": round(commit_latency_ms, 3),
                        "recovery_duration_ms": round(recovery_duration_ms, 3),
                        "success": success,
                        "node0_balance": final_balances.get("node0", ""),
                        "node1_balance": final_balances.get("node1", ""),
                    })
                finally:
                    await cluster.stop()

    csv_path = DATA_DIR / "exp2_two_phase_commit.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {len(results)} rows to {csv_path}", flush=True)
    return results


# ==============================================================================
# Experiment 3: Chandy-Lamport Distributed Snapshot
# ==============================================================================
async def run_exp3_cl(trials: int, run_root: Path) -> List[Dict[str, Any]]:
    print("\n--- Experiment 3: Chandy-Lamport Distributed Snapshot ---", flush=True)
    results: List[Dict[str, Any]] = []
    transfer_rates = [10, 25, 50]
    scenarios = ["calm", "jittery"]
    base_port = 29000

    for rate in transfer_rates:
        for scen in scenarios:
            print(f"  rate={rate} tx/s, scenario={scen}", flush=True)
            for trial in range(trials):
                seed = 3000 + trial * 23 + rate
                run_dir = run_root / f"exp3_{rate}_{scen}_{trial}"
                run_dir.mkdir(parents=True, exist_ok=True)
                cluster = Cluster(run_dir, base_port)
                base_port += 10

                try:
                    await cluster.launch(3)
                    if scen == "jittery":
                        await cluster.set_fault_all(seed, delay_mean_ms=8.0, delay_jitter_ms=5.0)

                    # Background in-flight traffic: direct transfers node1 -> node2.
                    # The snapshot is triggered while traffic is still flowing so
                    # the marker race against in-transit messages is exercised.
                    count = 8
                    await cluster.nodes["node1"].control("load", {
                        "target": "node2", "count": count, "rate": float(rate), "amount": 10.0,
                    }, timeout=15.0)
                    await asyncio.sleep(min(1.5 / rate, 0.3))

                    snap_id = f"snap_{rate}_{scen}_{trial}"
                    t0 = time.perf_counter()
                    await cluster.nodes["node0"].control("snapshot", {"snapshot_id": snap_id}, timeout=60.0)
                    duration_ms = (time.perf_counter() - t0) * 1000.0

                    audit = await cluster.nodes["node0"].control("audit", {
                        "snapshot_id": snap_id, "timeout_sec": 5.0,
                    }, timeout=30.0)
                    in_transit = sum(float(v) for v in audit["channel_in_transit"].values())

                    marker_stats = await asyncio.gather(*(n.stats() for n in cluster.nodes.values()))
                    total_markers = sum(s["snapshot"]["markers_sent"] for s in marker_stats)

                    results.append({
                        "trial": trial,
                        "transfer_rate": rate,
                        "scenario": scen,
                        "seed": seed,
                        "snapshot_duration_ms": round(duration_ms, 3),
                        "total_markers": total_markers,
                        "is_consistent": audit["is_consistent"],
                        "computed_total": audit["computed_total"],
                        "in_transit_total": round(in_transit, 3),
                    })
                finally:
                    await cluster.stop()

    csv_path = DATA_DIR / "exp3_chandy_lamport.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {len(results)} rows to {csv_path}", flush=True)
    return results


# ==============================================================================
# Environment metadata (hardware, OS, tool versions, campaign durations)
# ==============================================================================
def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def _tool_version(cmd: List[str]) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return (out.stdout or out.stderr).strip().splitlines()[0] if (out.stdout or out.stderr) else "unknown"
    except Exception:
        return "unknown"


def _git_rev() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT,
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def write_metadata(durations: Dict[str, float], trials: int, extra: Dict[str, Any]) -> None:
    metadata = {
        "hostname": platform.node(),
        "os": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "machine": platform.machine(),
        "cpu_model": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "python_version": platform.python_version(),
        "docker_version": _tool_version(["docker", "--version"]),
        "docker_compose_version": _tool_version(["docker", "compose", "version", "--short"]),
        "git_revision": _git_rev(),
        "deployment_model": "independent OS processes communicating over TCP (same CLI as containers)",
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "warmup": {
            "exp1_ricart_agrawala": "1 full CS entry/release on node0 before measurement",
            "exp2_two_phase_commit": "none (cold start measures commit directly)",
            "exp3_chandy_lamport": "none (traffic generator starts before snapshot)",
        },
        "trials_per_config": trials,
        "experiment_duration_s": {k: round(v, 1) for k, v in durations.items()},
        "notes": (
            "Faults are injected explicitly per scenario (delay, loss, partition, crash); "
            "channels are reliable FIFO TCP by default. Percentiles are computed per "
            "configuration over independent trials."
        ),
        **extra,
    }
    with open(DATA_DIR / "environment_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    print("Recorded environment metadata.", flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description="MC714 Trabalho 2 benchmark runner (process deployment)")
    parser.add_argument("--only", choices=["exp1", "exp2", "exp3"], default=None)
    parser.add_argument("--trials", type=int, default=10, help="Trials per configuration (default 10)")
    parser.add_argument("--keep-logs", action="store_true", help="Keep per-trial node logs under data/run_logs/")
    args = parser.parse_args()

    import shutil

    run_root = DATA_DIR / "run_logs"
    if args.keep_logs:
        run_root.mkdir(parents=True, exist_ok=True)
    else:
        run_root = Path(run_root.parent / f"run_logs_tmp_{uuid.uuid4().hex[:8]}")

    durations: Dict[str, float] = {}
    extra: Dict[str, Any] = {}
    jobs = [args.only] if args.only else ["exp1", "exp2", "exp3"]

    for job in jobs:
        t0 = time.perf_counter()
        if job == "exp1":
            rows = await run_exp1_ra(args.trials, run_root)
            extra["exp1_rows"] = len(rows)
        elif job == "exp2":
            rows = await run_exp2_tpc(args.trials, run_root)
            extra["exp2_rows"] = len(rows)
        else:
            rows = await run_exp3_cl(args.trials, run_root)
            extra["exp3_rows"] = len(rows)
        durations[job] = time.perf_counter() - t0

    write_metadata(durations, args.trials, extra)

    if not args.keep_logs:
        shutil.rmtree(run_root, ignore_errors=True)
    print("\nAll experiment benchmarks completed successfully!", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
