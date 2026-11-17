"""Node-level integration tests: balance reservations and RA-protected transfers."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Dict, Tuple

from distrib_ledger.node import DistributedNode


class TestNodeIntegration(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="node_test_")
        self.base_port = 19600
        self.num_nodes = 3
        self.peer_addrs: Dict[str, Tuple[str, int]] = {
            f"node{i}": ("127.0.0.1", self.base_port + i) for i in range(self.num_nodes)
        }
        self.nodes: Dict[str, DistributedNode] = {}
        for nid, (host, port) in self.peer_addrs.items():
            node = DistributedNode(
                node_id=nid,
                host=host,
                port=port,
                peer_addresses=self.peer_addrs,
                wal_dir=Path(self.tmpdir) / nid,
                initial_balance=1000.0,
            )
            self.nodes[nid] = node
            await node.start()

    async def asyncTearDown(self):
        for node in self.nodes.values():
            try:
                await node.stop()
            except Exception:
                pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    async def test_concurrent_transfers_cannot_double_spend(self):
        """Two concurrent 2PC transfers exceeding the balance: exactly one commits."""
        results = await asyncio.gather(
            self.nodes["node0"].transfer_via_2pc("node1", 800.0, use_cs=False),
            self.nodes["node0"].transfer_via_2pc("node1", 800.0, use_cs=False),
        )

        committed = sum(1 for r in results if r)
        self.assertEqual(committed, 1, f"Exactly one of two conflicting transfers may commit, got {results}")
        self.assertEqual(self.nodes["node0"].balance, 200.0)
        self.assertEqual(self.nodes["node1"].balance, 1800.0)
        self.assertEqual(self.nodes["node0"].available_balance(), 200.0, "Reservation must be released after decision")

    async def test_transfer_is_protected_by_ra_critical_section(self):
        """App-level transfers serialize through the Ricart-Agrawala CS by default."""
        ok = await self.nodes["node0"].transfer_via_2pc("node1", 150.0, use_cs=True)
        self.assertTrue(ok)
        self.assertEqual(self.nodes["node0"].balance, 850.0)
        self.assertEqual(self.nodes["node1"].balance, 1150.0)
        self.assertEqual(self.nodes["node0"].ricart_agrawala.stats["cs_entries"], 1)

    async def test_transfer_releases_cs_after_abort(self):
        """An aborted transfer must still release the critical section."""
        # node0 tries to spend more than it has: 2PC aborts during voting.
        ok = await self.nodes["node0"].transfer_via_2pc("node1", 5000.0, use_cs=True)
        self.assertFalse(ok)
        ra = self.nodes["node0"].ricart_agrawala
        self.assertEqual(ra.state.value, "RELEASED", "CS must be released after an aborted transfer")
        self.assertEqual(self.nodes["node0"].balance, 1000.0)
        self.assertEqual(self.nodes["node0"].available_balance(), 1000.0)


if __name__ == "__main__":
    unittest.main()
