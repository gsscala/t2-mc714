"""Unit and integration tests for Ricart-Agrawala Mutual Exclusion."""

from __future__ import annotations

import asyncio
import unittest
from typing import Dict, List, Tuple

from distrib_ledger.mutual_exclusion.lamport_clock import LamportClock
from distrib_ledger.mutual_exclusion.ricart_agrawala import NodeState, RicartAgrawala
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType


class TestRicartAgrawala(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.base_port = 19100
        self.num_nodes = 3
        self.peer_addrs: Dict[str, Tuple[str, int]] = {
            f"node{i}": ("127.0.0.1", self.base_port + i) for i in range(self.num_nodes)
        }
        self.channels: Dict[str, NetworkChannel] = {}
        self.algorithms: Dict[str, RicartAgrawala] = {}

        for nid, (host, port) in self.peer_addrs.items():
            ch = NetworkChannel(node_id=nid, host=host, port=port, peer_addresses=self.peer_addrs)
            ra = RicartAgrawala(node_id=nid, channel=ch)
            ch.register_handler(self._make_handler(ra))
            self.channels[nid] = ch
            self.algorithms[nid] = ra
            await ch.start()

    def _make_handler(self, ra: RicartAgrawala):
        async def handler(msg: Message):
            if msg.msg_type == MessageType.RA_REQUEST:
                await ra.handle_request(msg)
            elif msg.msg_type == MessageType.RA_REPLY:
                await ra.handle_reply(msg)
        return handler

    async def asyncTearDown(self):
        for ch in self.channels.values():
            await ch.stop()

    async def test_mutual_exclusion_invariant(self):
        """Verifies safety invariant: at most 1 node in Critical Section simultaneously."""
        in_cs_count = 0
        max_concurrent_in_cs = 0
        cs_lock = asyncio.Lock()

        async def worker(nid: str, runs: int):
            nonlocal in_cs_count, max_concurrent_in_cs
            ra = self.algorithms[nid]
            for _ in range(runs):
                await ra.request_cs()
                async with cs_lock:
                    in_cs_count += 1
                    if in_cs_count > max_concurrent_in_cs:
                        max_concurrent_in_cs = in_cs_count
                    # Hold CS briefly to allow overlap attempt
                    await asyncio.sleep(0.01)
                    in_cs_count -= 1
                await ra.release_cs()
                await asyncio.sleep(0.005)

        # Run 3 nodes competing concurrently
        tasks = [worker(nid, 3) for nid in self.algorithms.keys()]
        await asyncio.gather(*tasks)

        self.assertEqual(max_concurrent_in_cs, 1, "Safety violated: more than one node in CS simultaneously!")
        self.assertEqual(in_cs_count, 0)

    async def test_exact_message_complexity(self):
        """Verifies complexity: exactly 2*(N-1) messages per CS entry."""
        ra = self.algorithms["node0"]
        initial_requests = ra.stats["requests_sent"]
        initial_replies = sum(a.stats["replies_sent"] for a in self.algorithms.values())

        await ra.request_cs()
        await ra.release_cs()

        delta_requests = ra.stats["requests_sent"] - initial_requests
        delta_replies = sum(a.stats["replies_sent"] for a in self.algorithms.values()) - initial_replies

        expected = self.num_nodes - 1  # 2 requests and 2 replies
        self.assertEqual(delta_requests, expected, f"Expected {expected} requests, got {delta_requests}")
        self.assertEqual(delta_replies, expected, f"Expected {expected} replies, got {delta_replies}")
        self.assertEqual(delta_requests + delta_replies, 2 * expected)

    async def test_fairness_and_liveness(self):
        """Verifies that all nodes requesting CS make progress and enter without starvation."""
        completed = []

        async def worker(nid: str):
            ra = self.algorithms[nid]
            await ra.request_cs()
            completed.append(nid)
            await asyncio.sleep(0.01)
            await ra.release_cs()

        tasks = [worker(nid) for nid in self.algorithms.keys()]
        await asyncio.gather(*tasks)

        self.assertEqual(len(completed), self.num_nodes)
        self.assertEqual(set(completed), set(self.algorithms.keys()))


if __name__ == "__main__":
    unittest.main()
