"""Unit and integration tests for Chandy-Lamport Distributed Snapshot."""

from __future__ import annotations

import asyncio
import unittest
from typing import Dict, List, Tuple

from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType
from distrib_ledger.snapshot.auditor import SnapshotAuditor
from distrib_ledger.snapshot.chandy_lamport import ChandyLamport


class TestChandyLamport(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.base_port = 19300
        self.num_nodes = 3
        self.peer_addrs: Dict[str, Tuple[str, int]] = {
            f"node{i}": ("127.0.0.1", self.base_port + i) for i in range(self.num_nodes)
        }
        self.balances: Dict[str, float] = {f"node{i}": 1000.0 for i in range(self.num_nodes)}
        self.channels: Dict[str, NetworkChannel] = {}
        self.snapshots: Dict[str, ChandyLamport] = {}
        self.auditor = SnapshotAuditor(
            all_nodes=list(self.peer_addrs.keys()),
            expected_total_money=3000.0,
        )

        for nid, (host, port) in self.peer_addrs.items():
            ch = NetworkChannel(node_id=nid, host=host, port=port, peer_addresses=self.peer_addrs)
            cl = ChandyLamport(
                node_id=nid,
                channel=ch,
                get_local_state_cb=self._make_get_state(nid),
                peers=list(self.peer_addrs.keys()),
                auditor_id="node0",  # node0 hosts the auditor
            )
            ch.register_handler(self._make_handler(nid, cl))
            self.channels[nid] = ch
            self.snapshots[nid] = cl
            await ch.start()

    def _make_get_state(self, nid: str):
        return lambda: {"balance": self.balances[nid]}

    def _make_handler(self, nid: str, cl: ChandyLamport):
        async def handler(msg: Message):
            mtype = msg.msg_type
            if mtype == MessageType.CL_MARKER:
                await cl.handle_marker(msg)
            elif mtype == MessageType.CL_TRANSFER:
                await cl.intercept_incoming_app_message(msg.sender_id, msg.payload)
                amt = float(msg.payload.get("amount", 0.0))
                self.balances[nid] += amt
            elif mtype == MessageType.CL_SNAPSHOT_REPORT:
                if nid == "node0":
                    await self.auditor.handle_report(msg)
        return handler

    async def asyncTearDown(self):
        for ch in self.channels.values():
            await ch.stop()

    async def test_snapshot_idle_system(self):
        """Verifies snapshot on an idle system without in-flight traffic."""
        snap_id = "snap_idle_1"
        snap = await self.snapshots["node0"].initiate_snapshot(snap_id)

        # Wait for initiator to complete
        await asyncio.wait_for(snap.completion_event.wait(), timeout=3.0)
        self.assertTrue(snap.is_complete)

        # Verify through auditor
        report = await self.auditor.wait_and_verify(snap_id, timeout_sec=3.0)
        self.assertTrue(report.is_consistent)
        self.assertEqual(report.computed_total, 3000.0)
        self.assertEqual(len(report.missing_nodes), 0)

    async def test_snapshot_with_in_flight_transfers(self):
        """Verifies balance conservation with concurrent in-flight messages."""
        snap_id = "snap_active_1"

        # Apply intentional artificial delay on channel from node1 to node2 to guarantee in-flight transit
        self.channels["node1"].fault_injector.set_delay(mean_ms=20.0)

        # Deduct on sender and send transfer
        self.balances["node1"] -= 100.0
        transfer_msg = Message(
            msg_type=MessageType.CL_TRANSFER,
            sender_id="node1",
            timestamp=1,
            payload={"amount": 100.0, "target_id": "node2"},
        )

        # Send transfer first
        transfer_task = asyncio.create_task(self.channels["node1"].send_message("node2", transfer_msg))

        # Immediately initiate snapshot from node0
        snap = await self.snapshots["node0"].initiate_snapshot(snap_id)

        await transfer_task
        await asyncio.wait_for(snap.completion_event.wait(), timeout=3.0)

        # Wait and verify via auditor
        report = await self.auditor.wait_and_verify(snap_id, timeout_sec=3.0)
        self.assertTrue(report.is_consistent, f"Snapshot inconsistent! Details: {report}")
        self.assertAlmostEqual(report.computed_total, 3000.0, places=2)

    async def test_snapshot_stress_under_concurrent_traffic(self):
        """Repeated snapshots under jittered concurrent transfers never double count.

        Regression test for the marker-overtaking race: a transfer whose debit
        happens after the sender's state capture must never be both applied at
        the receiver and recorded as in-transit. Uses full DistributedNode
        instances so the marker ordering gate is exercised exactly as in
        production.
        """
        import shutil
        import tempfile
        from pathlib import Path

        from distrib_ledger.node import DistributedNode

        tmpdir = tempfile.mkdtemp(prefix="cl_stress_")
        base_port = 19520
        peer_addrs = {f"node{i}": ("127.0.0.1", base_port + i) for i in range(3)}
        nodes: dict[str, DistributedNode] = {}
        try:
            for nid, (host, port) in peer_addrs.items():
                node = DistributedNode(
                    node_id=nid,
                    host=host,
                    port=port,
                    peer_addresses=peer_addrs,
                    wal_dir=Path(tmpdir) / nid,
                    initial_balance=1000.0,
                    auditor_id="node0",
                )
                nodes[nid] = node
                await node.start()

            sender = nodes["node1"].fault_injector
            sender.set_delay(mean_ms=8.0, jitter_ms=5.0)
            sender.set_seed(99)

            for i in range(12):
                snap_id = f"snap_stress_{i}"

                # Traffic starts slightly before the snapshot and keeps flowing
                await nodes["node1"].start_background_load("node2", count=6, rate=60.0, amount=10.0)
                await asyncio.sleep(0.005)

                snap = await nodes["node0"].snapshot.initiate_snapshot(snap_id)
                await asyncio.wait_for(snap.completion_event.wait(), timeout=5.0)
                report = await nodes["node0"].auditor.wait_and_verify(snap_id, timeout_sec=5.0)
                self.assertTrue(
                    report.is_consistent,
                    f"Trial {i}: conservation violated! Details: {report}",
                )
                self.assertAlmostEqual(report.computed_total, 3000.0, places=2)

                # Let outstanding traffic drain so trials stay independent
                await asyncio.sleep(0.3)
        finally:
            for node in nodes.values():
                try:
                    await node.stop()
                except Exception:
                    pass
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
