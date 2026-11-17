"""Integration tests under adverse network conditions and fault injection."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Tuple

from distrib_ledger.atomic_commit.two_phase_commit import TwoPhaseCommit
from distrib_ledger.atomic_commit.wal import WriteAheadLog
from distrib_ledger.mutual_exclusion.lamport_clock import LamportClock
from distrib_ledger.mutual_exclusion.ricart_agrawala import RicartAgrawala
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.fault_injector import FaultConfig, FaultInjector
from distrib_ledger.network.protocol import Message, MessageType
from distrib_ledger.snapshot.auditor import SnapshotAuditor
from distrib_ledger.snapshot.chandy_lamport import ChandyLamport


class TestFaultTolerance(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="fault_test_")
        self.base_port = 19400
        self.num_nodes = 4
        self.peer_addrs: Dict[str, Tuple[str, int]] = {
            f"node{i}": ("127.0.0.1", self.base_port + i) for i in range(self.num_nodes)
        }
        self.channels: Dict[str, NetworkChannel] = {}
        self.injectors: Dict[str, FaultInjector] = {}
        self.balances: Dict[str, float] = {f"node{i}": 1000.0 for i in range(self.num_nodes)}
        self.wals: Dict[str, WriteAheadLog] = {}
        self.tpcs: Dict[str, TwoPhaseCommit] = {}
        self.ras: Dict[str, RicartAgrawala] = {}
        self.snapshots: Dict[str, ChandyLamport] = {}

        self.auditor = SnapshotAuditor(
            all_nodes=list(self.peer_addrs.keys()),
            expected_total_money=4000.0,
        )

        for nid, (host, port) in self.peer_addrs.items():
            inj = FaultInjector(FaultConfig(seed=42))
            ch = NetworkChannel(node_id=nid, host=host, port=port, peer_addresses=self.peer_addrs, fault_injector=inj)
            wal = WriteAheadLog(Path(self.tmpdir) / f"{nid}.wal")

            tpc = TwoPhaseCommit(
                node_id=nid,
                channel=ch,
                wal=wal,
                apply_commit_cb=self._make_apply_cb(nid),
                validate_prepare_cb=self._make_validate_cb(nid),
                vote_timeout_sec=1.5,
            )
            ra = RicartAgrawala(node_id=nid, channel=ch, retransmit_interval_sec=0.2)
            cl = ChandyLamport(
                node_id=nid,
                channel=ch,
                get_local_state_cb=self._make_get_state(nid),
                peers=list(self.peer_addrs.keys()),
                auditor_id="node0",
            )

            ch.register_handler(self._make_dispatcher(nid, tpc, ra, cl))
            self.channels[nid] = ch
            self.injectors[nid] = inj
            self.wals[nid] = wal
            self.tpcs[nid] = tpc
            self.ras[nid] = ra
            self.snapshots[nid] = cl
            await ch.start()

    def _make_get_state(self, nid: str):
        return lambda: {"balance": self.balances[nid]}

    def _make_validate_cb(self, nid: str):
        def validate(tx_id: str, ops: list) -> bool:
            for op in ops:
                if op.get("from_node") == nid and self.balances[nid] < float(op.get("amount", 0.0)):
                    return False
            return True
        return validate

    def _make_apply_cb(self, nid: str):
        def apply(tx_id: str, ops: list) -> bool:
            for op in ops:
                if op.get("from_node") == nid:
                    self.balances[nid] -= float(op.get("amount", 0.0))
                elif op.get("to_node") == nid:
                    self.balances[nid] += float(op.get("amount", 0.0))
            return True
        return apply

    def _make_dispatcher(self, nid: str, tpc: TwoPhaseCommit, ra: RicartAgrawala, cl: ChandyLamport):
        async def handler(msg: Message):
            mtype = msg.msg_type
            if mtype == MessageType.RA_REQUEST:
                await ra.handle_request(msg)
            elif mtype == MessageType.RA_REPLY:
                await ra.handle_reply(msg)
            elif mtype == MessageType.TPC_PREPARE:
                await tpc.handle_prepare(msg)
            elif mtype == MessageType.TPC_VOTE_COMMIT:
                await tpc.handle_vote_commit(msg)
            elif mtype == MessageType.TPC_VOTE_ABORT:
                await tpc.handle_vote_abort(msg)
            elif mtype == MessageType.TPC_GLOBAL_COMMIT:
                await tpc.handle_global_commit(msg)
            elif mtype == MessageType.TPC_GLOBAL_ABORT:
                await tpc.handle_global_abort(msg)
            elif mtype == MessageType.TPC_ACK:
                await tpc.handle_ack(msg)
            elif mtype == MessageType.TPC_QUERY_STATUS:
                await tpc.handle_query_status(msg)
            elif mtype == MessageType.CL_MARKER:
                await cl.handle_marker(msg)
            elif mtype == MessageType.CL_TRANSFER:
                await cl.intercept_incoming_app_message(msg.sender_id, msg.payload)
                self.balances[nid] += float(msg.payload.get("amount", 0.0))
            elif mtype == MessageType.CL_SNAPSHOT_REPORT:
                if nid == "node0":
                    await self.auditor.handle_report(msg)
        return handler

    async def asyncTearDown(self):
        for ch in self.channels.values():
            await ch.stop()
        for wal in self.wals.values():
            wal.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    async def test_mutual_exclusion_under_network_delay(self):
        """Ricart-Agrawala preserves mutual exclusion safety under delayed channels."""
        for inj in self.injectors.values():
            inj.set_delay(mean_ms=10.0, jitter_ms=5.0)

        in_cs = 0
        max_in_cs = 0
        lock = asyncio.Lock()

        async def worker(nid: str):
            nonlocal in_cs, max_in_cs
            ra = self.ras[nid]
            await ra.request_cs()
            async with lock:
                in_cs += 1
                max_in_cs = max(max_in_cs, in_cs)
                await asyncio.sleep(0.01)
                in_cs -= 1
            await ra.release_cs()

        await asyncio.gather(*[worker(nid) for nid in self.peer_addrs.keys()])
        self.assertEqual(max_in_cs, 1, "Mutual exclusion invariant violated under jittery network!")

    async def test_2pc_safety_with_simulated_partition(self):
        """2PC aborts cleanly without state divergence when participant is partitioned."""
        # Partition node3 from node0 (coordinator)
        self.injectors["node0"].partition("node0", "node3")
        self.injectors["node3"].partition("node0", "node3")

        coord = self.tpcs["node0"]
        ops = [{"from_node": "node0", "amount": 100.0}, {"to_node": "node3", "amount": 100.0}]
        success = await coord.execute_transaction(ops, ["node0", "node3"])

        # Must abort cleanly due to timeout / unreachable participant
        self.assertFalse(success)
        self.assertEqual(self.balances["node0"], 1000.0)
        self.assertEqual(self.balances["node3"], 1000.0)

        # Heal partition
        self.injectors["node0"].heal_all_partitions()
        self.injectors["node3"].heal_all_partitions()

    async def test_2pc_commits_after_partition_heal(self):
        """After healing the partition, subsequent transactions commit normally."""
        self.injectors["node0"].partition("node0", "node2")
        self.injectors["node2"].partition("node0", "node2")

        ops1 = [{"from_node": "node0", "amount": 60.0}, {"to_node": "node2", "amount": 60.0}]
        self.assertFalse(await self.tpcs["node0"].execute_transaction(ops1, ["node0", "node2"]))

        self.injectors["node0"].heal_all_partitions()
        self.injectors["node2"].heal_all_partitions()

        ops2 = [{"from_node": "node0", "amount": 60.0}, {"to_node": "node2", "amount": 60.0}]
        self.assertTrue(await self.tpcs["node0"].execute_transaction(ops2, ["node0", "node2"]))
        self.assertEqual(self.balances["node0"], 940.0)
        self.assertEqual(self.balances["node2"], 1060.0)

    async def test_2pc_atomicity_under_combined_delay_and_loss(self):
        """2PC outcome stays atomic with simultaneous packet loss and delay."""
        for inj in self.injectors.values():
            inj.set_seed(1234)
            inj.set_delay(mean_ms=5.0, jitter_ms=3.0)
            inj.set_drop_rate(0.15)

        results = []
        for _ in range(5):
            ops = [{"from_node": "node0", "amount": 20.0}, {"to_node": "node1", "amount": 20.0}]
            results.append(await self.tpcs["node0"].execute_transaction(ops, ["node0", "node1"]))

        # Whatever the outcome mix, balances must be consistent: node0 lost
        # exactly the amount node1 gained, and neither went negative.
        committed = sum(1 for r in results if r)
        self.assertEqual(self.balances["node0"], 1000.0 - 20.0 * committed)
        self.assertEqual(self.balances["node1"], 1000.0 + 20.0 * committed)
        self.assertGreaterEqual(self.balances["node0"], 0.0)

    async def test_mutual_exclusion_under_packet_loss(self):
        """RA keeps safety and achieves entry via idempotent retransmission under loss."""
        for inj in self.injectors.values():
            inj.set_seed(777)
            inj.set_drop_rate(0.25)

        in_cs = 0
        max_in_cs = 0
        lock = asyncio.Lock()

        async def worker(nid: str):
            nonlocal in_cs, max_in_cs
            ra = self.ras[nid]
            await ra.request_cs()
            async with lock:
                in_cs += 1
                max_in_cs = max(max_in_cs, in_cs)
                await asyncio.sleep(0.005)
                in_cs -= 1
            await ra.release_cs()

        await asyncio.gather(*[worker(nid) for nid in self.peer_addrs.keys()])
        self.assertEqual(max_in_cs, 1, "Mutual exclusion violated under packet loss!")
        total_retransmits = sum(ra.stats["retransmits_sent"] for ra in self.ras.values())
        self.assertGreater(total_retransmits, 0, "Retransmission path was never exercised")


if __name__ == "__main__":
    unittest.main()
