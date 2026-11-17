"""Unit and integration tests for Two-Phase Commit with Write-Ahead Logging and Recovery."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Dict, List, Tuple

from distrib_ledger.atomic_commit.recovery import RecoveryManager
from distrib_ledger.atomic_commit.two_phase_commit import TwoPhaseCommit, TxStatus
from distrib_ledger.atomic_commit.wal import WalRecordType, WriteAheadLog
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType


class TestTwoPhaseCommit(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="wal_test_")
        self.base_port = 19200
        self.num_nodes = 3
        self.peer_addrs: Dict[str, Tuple[str, int]] = {
            f"node{i}": ("127.0.0.1", self.base_port + i) for i in range(self.num_nodes)
        }
        self.balances: Dict[str, float] = {f"node{i}": 1000.0 for i in range(self.num_nodes)}
        self.channels: Dict[str, NetworkChannel] = {}
        self.wals: Dict[str, WriteAheadLog] = {}
        self.tpcs: Dict[str, TwoPhaseCommit] = {}

        for nid, (host, port) in self.peer_addrs.items():
            ch = NetworkChannel(node_id=nid, host=host, port=port, peer_addresses=self.peer_addrs)
            wal = WriteAheadLog(Path(self.tmpdir) / f"{nid}.wal")
            tpc = TwoPhaseCommit(
                node_id=nid,
                channel=ch,
                wal=wal,
                apply_commit_cb=self._make_apply_cb(nid),
                validate_prepare_cb=self._make_validate_cb(nid),
            )
            ch.register_handler(self._make_handler(tpc))
            self.channels[nid] = ch
            self.wals[nid] = wal
            self.tpcs[nid] = tpc
            await ch.start()

    def _make_validate_cb(self, nid: str):
        def validate(tx_id: str, ops: list) -> bool:
            for op in ops:
                if op.get("from_node") == nid:
                    amt = float(op.get("amount", 0.0))
                    if self.balances[nid] < amt:
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

    def _make_handler(self, tpc: TwoPhaseCommit):
        async def handler(msg: Message):
            mtype = msg.msg_type
            if mtype == MessageType.TPC_PREPARE:
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
        return handler

    async def asyncTearDown(self):
        for ch in self.channels.values():
            await ch.stop()
        for wal in self.wals.values():
            wal.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    async def test_successful_commit(self):
        """Verifies normal 2PC execution with unanimous commit."""
        coord = self.tpcs["node0"]
        ops = [
            {"from_node": "node0", "amount": 150.0},
            {"to_node": "node1", "amount": 150.0},
        ]
        participants = ["node0", "node1"]

        success = await coord.execute_transaction(ops, participants)
        self.assertTrue(success)
        self.assertEqual(self.balances["node0"], 850.0)
        self.assertEqual(self.balances["node1"], 1150.0)
        self.assertEqual(self.balances["node2"], 1000.0)

    async def test_aborted_transaction_insufficient_funds(self):
        """Verifies atomic rollback when a participant rejects prepare."""
        coord = self.tpcs["node0"]
        ops = [
            {"from_node": "node0", "amount": 5000.0},  # Exceeds initial balance 1000
            {"to_node": "node1", "amount": 5000.0},
        ]
        participants = ["node0", "node1"]

        success = await coord.execute_transaction(ops, participants)
        self.assertFalse(success)
        # Balances must be untouched
        self.assertEqual(self.balances["node0"], 1000.0)
        self.assertEqual(self.balances["node1"], 1000.0)

    async def test_participant_crash_and_recovery(self):
        """Verifies crash-recovery when participant crashes while PREPARED."""
        # Stop node1 to simulate crash
        await self.channels["node1"].stop()
        self.wals["node1"].close()

        tx_id = "tx_crash_test_1"
        ops = [{"from_node": "node0", "amount": 200.0}, {"to_node": "node1", "amount": 200.0}]

        # Write PREPARED to node1's WAL
        node1_wal_path = Path(self.tmpdir) / "node1.wal"
        crash_wal = WriteAheadLog(node1_wal_path)
        crash_wal.append(
            WalRecordType.PREPARED,
            tx_id,
            {"operations": ops, "coordinator": "node0"},
        )
        crash_wal.close()

        # Coordinator recorded commit
        self.wals["node0"].append(WalRecordType.COMMITTED, tx_id, {"operations": ops})
        self.tpcs["node0"].completed_transactions[tx_id] = TxStatus.COMMITTED

        # Reboot node1
        reboot_wal = WriteAheadLog(node1_wal_path)
        host, port = self.peer_addrs["node1"]
        reboot_ch = NetworkChannel(
            node_id="node1",
            host=host,
            port=port,
            peer_addresses=self.peer_addrs,
        )
        await reboot_ch.start()

        reboot_balance = 1000.0
        def reboot_apply(tid: str, oplist: list):
            nonlocal reboot_balance
            for op in oplist:
                if op.get("to_node") == "node1":
                    reboot_balance += float(op.get("amount", 0.0))
            return True

        reboot_tpc = TwoPhaseCommit(
            node_id="node1",
            channel=reboot_ch,
            wal=reboot_wal,
            apply_commit_cb=reboot_apply,
        )
        recovery_mgr = RecoveryManager(
            node_id="node1",
            tpc=reboot_tpc,
            wal=reboot_wal,
            channel=reboot_ch,
            peers=list(self.peer_addrs.keys()),
        )

        results = await recovery_mgr.recover()
        self.assertEqual(results.get(tx_id), "COMMITTED")
        self.assertEqual(reboot_balance, 1200.0)

        await reboot_ch.stop()
        reboot_wal.close()

    async def test_wal_torn_line_is_truncated_not_fatal(self):
        """A crash mid-append leaves a torn record; recovery must truncate, not crash."""
        wal_path = Path(self.tmpdir) / "torn.wal"
        wal = WriteAheadLog(wal_path)
        wal.append(WalRecordType.PREPARED, "tx_ok_1", {"operations": [], "coordinator": "node0"})
        # Simulate a torn append (process died mid-write): partial JSON, no newline
        wal._file.write('{"type": "COMM')
        wal._file.flush()
        wal.close()

        wal2 = WriteAheadLog(wal_path)
        recovered = wal2.recover()
        self.assertIn("tx_ok_1", recovered)
        self.assertEqual(recovered["tx_ok_1"]["status"], "PREPARED")

        # Log remains usable for new appends after the truncation
        wal2.append(WalRecordType.COMMITTED, "tx_ok_2", {})
        recovered2 = wal2.recover()
        self.assertIn("tx_ok_2", recovered2)
        wal2.close()

    async def test_recovery_reapplies_committed_exactly_once(self):
        """COMMITTED without APPLIED is re-applied once; APPLIED makes it idempotent."""
        wal_path = Path(self.tmpdir) / "reapply.wal"
        wal = WriteAheadLog(wal_path)
        ops = [{"from_node": "node0", "amount": 200.0}, {"to_node": "node1", "amount": 200.0}]
        wal.append(WalRecordType.START_2PC, "tx_ra_1", {"operations": ops, "participants": ["node0", "node1"]})
        wal.append(WalRecordType.COMMITTED, "tx_ra_1", {"operations": ops, "participants": ["node0", "node1"]})

        applied_calls = []

        def apply_cb(tx_id: str, oplist: list) -> bool:
            applied_calls.append(tx_id)
            return True

        ch = NetworkChannel(node_id="node0", host="127.0.0.1", port=19777, peer_addresses={})
        await ch.start()
        tpc = TwoPhaseCommit(node_id="node0", channel=ch, wal=wal, apply_commit_cb=apply_cb)
        rm = RecoveryManager("node0", tpc, wal, ch, peers=["node0", "node1"])

        results = await rm.recover()
        self.assertEqual(results.get("tx_ra_1"), "COMMITTED")
        self.assertEqual(len(applied_calls), 1, "Apply must run exactly once after the first recovery")

        results2 = await rm.recover()
        self.assertEqual(results2.get("tx_ra_1"), "COMMITTED")
        self.assertEqual(len(applied_calls), 1, "Second recovery must not re-apply (APPLIED record)")
        await asyncio.sleep(0.1)  # let the fire-and-forget re-propagation task drain
        await ch.stop()
        wal.close()

    async def test_in_doubt_participant_never_aborts_unilaterally(self):
        """An unreachable coordinator leaves the participant IN_DOUBT, not ABORTED.

        Unilateral abort would diverge from a coordinator that durably decided
        COMMIT before crashing (atomicity violation). When the coordinator
        returns, its recovery re-propagates the decision and both sides converge.
        """
        tx_id = "tx_indoubt_1"
        ops = [{"from_node": "node0", "amount": 200.0}, {"to_node": "node1", "amount": 200.0}]

        # Coordinator durably decided COMMIT right before "crashing" (before
        # applying locally and before propagating the decision).
        self.wals["node0"].append(
            WalRecordType.START_2PC, tx_id, {"operations": ops, "participants": ["node0", "node1"]}
        )
        self.wals["node0"].append(
            WalRecordType.COMMITTED, tx_id, {"operations": ops, "participants": ["node0", "node1"]}
        )
        # Participant voted COMMIT and is stranded in-doubt
        self.wals["node1"].append(
            WalRecordType.PREPARED, tx_id, {"operations": ops, "coordinator": "node0"}
        )

        # Coordinator "crashes": its listener stops, queries from node1 fail.
        await self.channels["node0"].stop()

        rm1 = RecoveryManager(
            "node1", self.tpcs["node1"], self.wals["node1"], self.channels["node1"],
            peers=list(self.peer_addrs.keys()), resolve_rounds=1, resolve_query_timeout_sec=0.3,
        )
        results = await rm1.recover()
        self.assertEqual(results.get(tx_id), "IN_DOUBT")
        self.assertEqual(self.balances["node1"], 1000.0, "In-doubt participant must not change balances")

        # Decision still unresolved in the WAL (PREPARED, not ABORTED)
        still = self.wals["node1"].recover()
        self.assertEqual(still[tx_id]["status"], "PREPARED")

        # Coordinator comes back and replays its WAL: re-applies and re-propagates
        host, port = self.peer_addrs["node0"]
        coord_ch = NetworkChannel(node_id="node0", host=host, port=port, peer_addresses=self.peer_addrs)
        coord_tpc = TwoPhaseCommit(
            node_id="node0", channel=coord_ch, wal=self.wals["node0"],
            apply_commit_cb=self._make_apply_cb("node0"),
            validate_prepare_cb=self._make_validate_cb("node0"),
        )
        coord_ch.register_handler(self._make_handler(coord_tpc))
        await coord_ch.start()
        rm0 = RecoveryManager("node0", coord_tpc, self.wals["node0"], coord_ch, peers=list(self.peer_addrs.keys()))
        results0 = await rm0.recover()
        self.assertEqual(results0.get(tx_id), "COMMITTED")
        self.assertEqual(self.balances["node0"], 800.0, "Coordinator re-applies its committed operations")

        # Stranded participant adopts the re-propagated COMMIT
        for _ in range(100):
            if self.balances["node1"] == 1200.0:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.balances["node1"], 1200.0)

        # A recovery round on the participant resolves the tx without re-applying
        results1b = await rm1.recover()
        self.assertEqual(results1b.get(tx_id), "COMMITTED")
        self.assertEqual(self.balances["node1"], 1200.0, "Exactly-once apply: no double credit")

        await coord_ch.stop()

    async def test_duplicate_prepare_returns_recorded_vote(self):
        """A duplicate PREPARE must not re-validate: the original vote is re-sent."""
        tx_id = "tx_dup_1"
        votes: list = []

        async def capture(msg: Message) -> None:
            if msg.msg_type in (MessageType.TPC_VOTE_COMMIT, MessageType.TPC_VOTE_ABORT):
                votes.append((msg.msg_type, msg.payload.get("tx_id")))

        unsubscribe = self.channels["node0"].register_handler(capture)

        def make_prepare(operations: list) -> Message:
            return Message(
                msg_type=MessageType.TPC_PREPARE,
                sender_id="node0",
                timestamp=0,
                payload={"tx_id": tx_id, "operations": operations, "participants": ["node0", "node1"]},
            )

        ops_ok = [{"from_node": "node0", "amount": 100.0}, {"to_node": "node1", "amount": 100.0}]
        await self.tpcs["node1"].handle_prepare(make_prepare(ops_ok))

        # Drain the first vote
        for _ in range(50):
            if votes:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(votes), 1)
        self.assertEqual(votes[0][0], MessageType.TPC_VOTE_COMMIT)

        # Balance collapses before the duplicate PREPARE arrives
        self.balances["node1"] = 0.0
        await self.tpcs["node1"].handle_prepare(make_prepare(ops_ok))
        for _ in range(50):
            if len(votes) >= 2:
                break
            await asyncio.sleep(0.01)

        self.assertEqual(len(votes), 2)
        self.assertEqual(votes[1][0], MessageType.TPC_VOTE_COMMIT, "Duplicate PREPARE must re-send the recorded vote")
        unsubscribe()

    async def test_coordinator_committed_recovery_repropagates_decision(self):
        """Coordinator restart with COMMITTED re-applies and re-propagates the decision."""
        tx_id = "tx_reprop_1"
        ops = [{"from_node": "node0", "amount": 200.0}, {"to_node": "node1", "amount": 200.0}]

        self.wals["node0"].append(
            WalRecordType.START_2PC, tx_id, {"operations": ops, "participants": ["node0", "node1"]}
        )
        self.wals["node0"].append(
            WalRecordType.COMMITTED, tx_id, {"operations": ops, "participants": ["node0", "node1"]}
        )
        # Participant voted COMMIT and is stranded in-doubt
        self.wals["node1"].append(
            WalRecordType.PREPARED, tx_id, {"operations": ops, "coordinator": "node0"}
        )

        rm0 = RecoveryManager("node0", self.tpcs["node0"], self.wals["node0"], self.channels["node0"], peers=list(self.peer_addrs.keys()))
        results0 = await rm0.recover()
        self.assertEqual(results0.get(tx_id), "COMMITTED")
        self.assertEqual(self.balances["node0"], 800.0, "Coordinator re-applies its own committed operations")

        # Re-propagation is asynchronous; give the participant time to resolve
        for _ in range(100):
            if self.balances["node1"] == 1200.0:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.balances["node1"], 1200.0, "Stranded participant must adopt the re-propagated COMMIT")

        resolved = self.wals["node1"].recover()
        self.assertIn(resolved[tx_id]["status"], ("COMMITTED", "APPLIED"))


if __name__ == "__main__":
    unittest.main()
