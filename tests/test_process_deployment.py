"""End-to-end deployment test: each logical node runs as an independent OS process.

This exercises the deployment model required by the assignment (independent
processes communicating exclusively over TCP) without Docker, using the same
CLI entry point that compose.yaml uses inside containers.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from distrib_ledger.cli import send_control  # noqa: E402


class TestProcessDeployment(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="proc_test_")
        self.base_port = 21500
        self.num_nodes = 3
        self.peers = {f"node{i}": ("127.0.0.1", self.base_port + i) for i in range(self.num_nodes)}
        self.peer_args = ",".join(f"{nid}={h}:{p}" for nid, (h, p) in self.peers.items())
        self.procs: dict[str, subprocess.Popen] = {}
        self.node_id_by_proc: dict[int, str] = {}

    async def asyncTearDown(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _spawn(self, node_id: str) -> subprocess.Popen:
        host, port = self.peers[node_id]
        wal_dir = Path(self.tmpdir) / node_id
        log_file = open(Path(self.tmpdir) / f"{node_id}.log", "ab")
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "distrib_ledger.cli", "node",
                "--id", node_id,
                "--host", "127.0.0.1",
                "--port", str(port),
                "--peers", self.peer_args,
                "--wal-dir", str(wal_dir),
                "--balance", "1000",
                "--auditor", "node0",
            ],
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        self.procs[node_id] = proc
        return proc

    async def _wait_ready(self, node_id: str, timeout: float = 15.0) -> None:
        host, port = self.peers[node_id]
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            try:
                result = await send_control(host, port, "ping", timeout=2.0)
                if result.get("ok"):
                    return
            except (ConnectionError, OSError, TimeoutError):
                await asyncio.sleep(0.2)
        raise RuntimeError(f"Node {node_id} did not become ready")

    async def _get_stats(self, node_id: str) -> dict:
        host, port = self.peers[node_id]
        result = await send_control(host, port, "get_stats", timeout=10.0)
        if not result.get("ok"):
            raise RuntimeError(f"get_stats failed on {node_id}: {result.get('error')}")
        return result["result"]

    async def test_full_deployment_transfer_snapshot_and_crash_recovery(self):
        """Transfer (RA-protected), snapshot audit, and real SIGKILL-free crash recovery."""
        # 1. Launch the cluster: independent processes, TCP-only communication.
        for nid in self.peers:
            self._spawn(nid)
        for nid in self.peers:
            await self._wait_ready(nid)

        # 2. Atomic transfer protected by the Ricart-Agrawala critical section.
        host, port = self.peers["node0"]
        result = await send_control(host, port, "transfer", {
            "to": "node1", "amount": 150.0, "use_cs": True,
        }, timeout=60.0)
        self.assertTrue(result.get("ok"), f"transfer failed: {result.get('error')}")
        self.assertTrue(result["result"]["success"])

        stats0 = await self._get_stats("node0")
        stats1 = await self._get_stats("node1")
        self.assertEqual(stats0["balance"], 850.0)
        self.assertEqual(stats1["balance"], 1150.0)
        self.assertGreaterEqual(stats0["ra"]["cs_entries"], 1)

        # 3. Chandy-Lamport snapshot with audit on the idle system.
        result = await send_control(host, port, "snapshot", {"snapshot_id": "snap_proc_1"}, timeout=60.0)
        self.assertTrue(result.get("ok"), f"snapshot failed: {result.get('error')}")
        audit = await send_control(host, port, "audit", {"snapshot_id": "snap_proc_1"}, timeout=30.0)
        self.assertTrue(audit.get("ok"), f"audit failed: {audit.get('error')}")
        self.assertTrue(audit["result"]["is_consistent"])
        self.assertAlmostEqual(audit["result"]["computed_total"], 3000.0, places=4)

        # 4. Real crash: the coordinator process is killed right
        # after collecting votes (before the decision). The node dies without
        # replying, so the connection error below is the expected outcome.
        crash_tx = "tx_proc_crash_1"
        proc0 = self.procs["node0"]
        try:
            await send_control(host, port, "transfer_then_crash", {
                "to": "node1", "amount": 100.0, "tx_id": crash_tx, "crash_point": "before_decision",
            }, timeout=15.0)
        except (ConnectionError, OSError):
            pass  # expected: the coordinator exits mid-protocol

        # The process must die by itself (os._exit(9) inside the protocol hook).
        for _ in range(100):
            if proc0.poll() is not None:
                break
            await asyncio.sleep(0.05)
        self.assertIsNotNone(proc0.poll(), "Coordinator process should have crashed")

        # Relaunch on the same WAL directory: recovery runs during start.
        self._spawn("node0")
        await self._wait_ready("node0")

        stats0 = await self._get_stats("node0")
        self.assertEqual(stats0["last_recovery"].get(crash_tx), "ABORTED", "Presumed Abort must resolve the in-flight tx")
        self.assertEqual(stats0["balance"], 850.0)
        stats1 = await self._get_stats("node1")
        self.assertEqual(stats1["balance"], 1150.0, "Participant must not commit without the coordinator's decision")

        # 5. System remains operational after recovery.
        result = await send_control(host, port, "transfer", {
            "to": "node2", "amount": 50.0, "use_cs": False,
        }, timeout=60.0)
        self.assertTrue(result["result"]["success"])
        stats2 = await self._get_stats("node2")
        self.assertEqual(stats2["balance"], 1050.0)


if __name__ == "__main__":
    unittest.main()
