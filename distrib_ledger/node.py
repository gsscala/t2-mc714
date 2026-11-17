"""Distributed Ledger Logical Node Process integrating Ricart-Agrawala, 2PC, and Chandy-Lamport."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from distrib_ledger.atomic_commit.recovery import RecoveryManager
from distrib_ledger.atomic_commit.two_phase_commit import TwoPhaseCommit
from distrib_ledger.atomic_commit.wal import WalRecordType, WriteAheadLog
from distrib_ledger.mutual_exclusion.lamport_clock import LamportClock
from distrib_ledger.mutual_exclusion.ricart_agrawala import RicartAgrawala
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.fault_injector import FaultInjector
from distrib_ledger.network.protocol import Message, MessageType
from distrib_ledger.snapshot.auditor import SnapshotAuditor
from distrib_ledger.snapshot.chandy_lamport import ChandyLamport

logger = logging.getLogger(__name__)


class DistributedNode:
    """Logical node in the distributed system cluster.

    Balance correctness model: ``balance`` is the committed value; funds reserved
    by in-flight 2PC transactions are tracked in ``_pending_out`` so concurrent
    transactions cannot double-spend (validation uses the available balance).
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peer_addresses: Dict[str, Tuple[str, int]],
        wal_dir: str | Path = "wal_data",
        initial_balance: float = 1000.0,
        fault_injector: Optional[FaultInjector] = None,
        auditor_id: Optional[str] = None,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addresses = peer_addresses
        self.peers = [p for p in peer_addresses.keys() if p != node_id]
        self.balance = float(initial_balance)
        self._initial_balance = float(initial_balance)
        self.auditor_id = auditor_id

        self.fault_injector = fault_injector or FaultInjector()
        self.channel = NetworkChannel(
            node_id=node_id,
            host=host,
            port=port,
            peer_addresses=peer_addresses,
            fault_injector=self.fault_injector,
        )

        # Logical Clock
        self.clock = LamportClock(node_id)

        # Mutual Exclusion
        self.ricart_agrawala = RicartAgrawala(
            node_id=node_id,
            channel=self.channel,
            clock=self.clock,
            peers=self.peers,
        )

        # Write-Ahead Log
        wal_path = Path(wal_dir) / f"{node_id}.wal"
        self.wal = WriteAheadLog(wal_path)

        # Two-Phase Commit
        self.tpc = TwoPhaseCommit(
            node_id=node_id,
            channel=self.channel,
            wal=self.wal,
            apply_commit_cb=self._apply_2pc_commit,
            validate_prepare_cb=self._validate_2pc_prepare,
            abort_rollback_cb=self._rollback_2pc,
        )

        # Recovery Manager
        self.recovery_mgr = RecoveryManager(
            node_id=node_id,
            tpc=self.tpc,
            wal=self.wal,
            channel=self.channel,
            peers=self.peers,
        )

        # Chandy-Lamport Snapshot
        self.snapshot = ChandyLamport(
            node_id=node_id,
            channel=self.channel,
            get_local_state_cb=self._get_local_state,
            peers=self.peers,
            auditor_id=self.auditor_id,
        )

        # Auditor instance if this node acts as auditor
        self.auditor: Optional[SnapshotAuditor] = None
        if self.auditor_id == self.node_id:
            expected_total = float(initial_balance) * len(peer_addresses)
            self.auditor = SnapshotAuditor(
                all_nodes=list(peer_addresses.keys()),
                expected_total_money=expected_total,
            )

        # 2PC reservations (funds held for in-flight transactions) and vote memory.
        # Accessed synchronously only: atomic under the single-threaded event loop.
        self._pending_out: Dict[str, float] = {}
        self._prepared_votes: Dict[str, str] = {}

        self.last_recovery: Dict[str, str] = {}
        self.recovery_duration_ms: float = 0.0

        self._balance_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._load_tasks: List[asyncio.Task] = []
        self._running = False

    # ------------------------------------------------------------------
    # Local state and 2PC callbacks
    # ------------------------------------------------------------------
    def _get_local_state(self) -> Dict[str, Any]:
        return {"node_id": self.node_id, "balance": self.balance}

    def available_balance(self) -> float:
        """Committed balance minus funds reserved by in-flight transactions."""
        return self.balance - sum(self._pending_out.values())

    def _validate_2pc_prepare(self, tx_id: str, operations: List[Dict[str, Any]]) -> bool:
        """Validates and atomically reserves local debit capacity for a transaction.

        Duplicate validations for the same tx return the recorded vote instead of
        re-validating against a balance that may have changed in between.
        """
        if tx_id in self._prepared_votes:
            return self._prepared_votes[tx_id] == "COMMIT"

        total_debit = 0.0
        decision = "COMMIT"
        for op in operations:
            if op.get("from_node") == self.node_id:
                amount = float(op.get("amount", 0.0))
                if self.available_balance() - total_debit < amount:
                    logger.warning(
                        "Node %s cannot prepare tx %s: available %.2f < %.2f",
                        self.node_id, tx_id, self.available_balance(), amount,
                    )
                    decision = "ABORT"
                    break
                total_debit += amount

        if decision == "COMMIT" and total_debit > 0.0:
            self._pending_out[tx_id] = total_debit
        self._prepared_votes[tx_id] = decision
        return decision == "COMMIT"

    def _apply_2pc_commit(self, tx_id: str, operations: List[Dict[str, Any]]) -> bool:
        """Applies committed operations to local balance and releases the reservation."""
        for op in operations:
            if op.get("from_node") == self.node_id:
                amount = float(op.get("amount", 0.0))
                self.balance -= amount
                logger.info("Node %s debited %.2f (new balance: %.2f)", self.node_id, amount, self.balance)
            elif op.get("to_node") == self.node_id:
                amount = float(op.get("amount", 0.0))
                self.balance += amount
                logger.info("Node %s credited %.2f (new balance: %.2f)", self.node_id, amount, self.balance)
        self._pending_out.pop(tx_id, None)
        return True

    def _rollback_2pc(self, tx_id: str) -> None:
        """Releases the reservation of an aborted transaction."""
        self._pending_out.pop(tx_id, None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        """Starts node server, replays the WAL, then accepts control commands."""
        self.channel.register_handler(self._dispatch_message)
        await self.channel.start()
        self._running = True

        # Perform recovery on start. The node only becomes ready for control
        # commands once recovery completes, so drivers observe a consistent state.
        t0 = asyncio.get_running_loop().time()
        self.last_recovery = await self.recovery_mgr.recover()
        self._rebuild_balance_from_wal()
        self.recovery_duration_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
        if self.last_recovery:
            logger.info("Node %s recovery results: %s (%.2f ms)", self.node_id, self.last_recovery, self.recovery_duration_ms)
        self._ready.set()

    def _rebuild_balance_from_wal(self) -> None:
        """Rebuilds the durable balance by replaying the WAL in append order.

        A restarted process begins from the configured initial balance and must
        re-apply every durable state change exactly once: APPLIED 2PC operations
        (using the operations recorded earlier in the log) and direct transfers.
        Reservations of in-flight transactions do not survive a crash.
        """
        from distrib_ledger.atomic_commit.wal import WalRecordType

        self.balance = self._initial_balance
        self._pending_out.clear()
        operations_by_tx: Dict[str, List[Dict[str, Any]]] = {}
        replayed = 0
        for rec in self.wal.replay_records():
            if rec.record_type in (WalRecordType.START_2PC, WalRecordType.PREPARED, WalRecordType.COMMITTED):
                ops = rec.data.get("operations")
                if ops:
                    operations_by_tx[rec.tx_id] = ops
            elif rec.record_type == WalRecordType.APPLIED:
                for op in operations_by_tx.get(rec.tx_id, []):
                    self._apply_op(float(op.get("amount", 0.0)), op.get("from_node"), op.get("to_node"))
                replayed += 1
            elif rec.record_type == WalRecordType.DIRECT_TRANSFER:
                self._apply_op(
                    float(rec.data.get("amount", 0.0)),
                    rec.data.get("from_node"),
                    rec.data.get("to_node"),
                )
                replayed += 1
        if replayed:
            logger.info("Node %s rebuilt balance to %.2f from %d durable records", self.node_id, self.balance, replayed)

    def _apply_op(self, amount: float, from_node: Optional[str], to_node: Optional[str]) -> None:
        if from_node == self.node_id:
            self.balance -= amount
        elif to_node == self.node_id:
            self.balance += amount

    async def stop(self) -> None:
        """Stops node, cancels background work and flushes WAL."""
        self._running = False
        self._ready.clear()
        for task in self._load_tasks:
            task.cancel()
        await self.channel.stop()
        self.wal.close()

    def crash(self) -> None:
        """Simulates an abrupt crash (no cleanup, no flush). Process-level only."""
        logger.warning("Node %s CRASH (os._exit)", self.node_id)
        os._exit(9)

    # ------------------------------------------------------------------
    # Message dispatch
    # ------------------------------------------------------------------
    async def _dispatch_message(self, msg: Message) -> None:
        """Routes incoming messages to respective distributed algorithm modules."""
        mtype = msg.msg_type

        # Control plane
        if mtype == MessageType.CTRL_COMMAND:
            await self._handle_control(msg)

        # Ricart-Agrawala
        elif mtype == MessageType.RA_REQUEST:
            await self.ricart_agrawala.handle_request(msg)
        elif mtype == MessageType.RA_REPLY:
            await self.ricart_agrawala.handle_reply(msg)

        # Two-Phase Commit
        elif mtype == MessageType.TPC_PREPARE:
            await self.tpc.handle_prepare(msg)
        elif mtype == MessageType.TPC_VOTE_COMMIT:
            await self.tpc.handle_vote_commit(msg)
        elif mtype == MessageType.TPC_VOTE_ABORT:
            await self.tpc.handle_vote_abort(msg)
        elif mtype == MessageType.TPC_GLOBAL_COMMIT:
            await self.tpc.handle_global_commit(msg)
        elif mtype == MessageType.TPC_GLOBAL_ABORT:
            await self.tpc.handle_global_abort(msg)
        elif mtype == MessageType.TPC_ACK:
            await self.tpc.handle_ack(msg)
        elif mtype == MessageType.TPC_QUERY_STATUS:
            await self.tpc.handle_query_status(msg)

        # Chandy-Lamport
        elif mtype == MessageType.CL_MARKER:
            await self.snapshot.handle_marker(msg)
        elif mtype == MessageType.CL_TRANSFER:
            # First inform snapshot engine to capture if channel recording is active
            await self.snapshot.intercept_incoming_app_message(msg.sender_id, msg.payload)
            # Then execute balance transfer
            amount = float(msg.payload.get("amount", 0.0))
            async with self._balance_lock:
                self.balance += amount
            self.wal.append(
                WalRecordType.DIRECT_TRANSFER,
                f"dt_{msg.msg_id or uuid.uuid4().hex[:10]}",
                {"amount": amount, "from_node": msg.sender_id, "to_node": self.node_id},
            )
            logger.info("Node %s received direct transfer %.2f from %s (new balance: %.2f)", self.node_id, amount, msg.sender_id, self.balance)
        elif mtype == MessageType.CL_SNAPSHOT_REPORT:
            if self.auditor:
                await self.auditor.handle_report(msg)

    # ------------------------------------------------------------------
    # Application operations
    # ------------------------------------------------------------------
    async def send_direct_transfer(self, target_id: str, amount: float) -> bool:
        """Direct money transfer message (for snapshot in-flight testing)."""
        async with self._balance_lock:
            if self.available_balance() < amount:
                return False
            self.balance -= amount

        msg = Message(
            msg_type=MessageType.CL_TRANSFER,
            sender_id=self.node_id,
            timestamp=await self.clock.tick(),
            payload={"amount": amount, "target_id": target_id},
        )
        # Wait for any pending snapshot marker to this destination: a transfer
        # issued after our state capture must not overtake the marker on the wire.
        await self.snapshot.before_app_send(target_id)
        success = await self.channel.send_message(target_id, msg)
        if not success:
            # Revert local balance if failed immediately
            async with self._balance_lock:
                self.balance += amount
            return False
        self.wal.append(
            WalRecordType.DIRECT_TRANSFER,
            f"dt_{msg.msg_id or uuid.uuid4().hex[:10]}",
            {"amount": amount, "from_node": self.node_id, "to_node": target_id},
        )
        return True

    async def transfer_via_2pc(self, to_node_id: str, amount: float, use_cs: bool = True) -> bool:
        """Executes an atomic cross-node transfer using Two-Phase Commit.

        By default the coordination phase is protected by the Ricart-Agrawala
        critical section, so concurrent coordinations are totally ordered before
        they touch the participants' balances.
        """
        if to_node_id == self.node_id:
            raise ValueError("Cannot transfer to self via 2PC")

        operations = [
            {"from_node": self.node_id, "amount": amount},
            {"to_node": to_node_id, "amount": amount},
        ]
        participants = [self.node_id, to_node_id]

        if not use_cs:
            return await self.tpc.execute_transaction(operations, participants)

        await self.ricart_agrawala.request_cs()
        try:
            return await self.tpc.execute_transaction(operations, participants)
        finally:
            await self.ricart_agrawala.release_cs()

    async def start_background_load(self, target_id: str, count: int, rate: float, amount: float) -> int:
        """Spawns background direct transfers (used to keep channels busy during snapshots)."""

        async def worker() -> None:
            interval = (1.0 / rate) if rate > 0 else 0.0
            for _ in range(count):
                await self.send_direct_transfer(target_id, amount)
                if interval > 0:
                    await asyncio.sleep(interval)

        task = asyncio.create_task(worker())
        self._load_tasks.append(task)
        return count

    # ------------------------------------------------------------------
    # Control plane (driver / client commands)
    # ------------------------------------------------------------------
    async def _handle_control(self, msg: Message) -> None:
        payload = msg.payload
        op = payload.get("op", "")
        args = payload.get("args", {}) or {}
        request_id = payload.get("request_id", "")
        result: Dict[str, Any] = {}
        error: Optional[str] = None
        ok = True
        try:
            result = await self._run_control_op(op, args)
        except Exception as e:  # noqa: BLE001 - report any op failure to the caller
            ok = False
            error = f"{type(e).__name__}: {e}"
            logger.error("Node %s control op %s failed: %s", self.node_id, op, e)

        reply = Message(
            msg_type=MessageType.CTRL_RESULT,
            sender_id=self.node_id,
            timestamp=0,
            payload={"request_id": request_id, "op": op, "ok": ok, "result": result, "error": error},
        )
        if msg.reply_conn:
            await self.channel.send_reply(msg.reply_conn, reply)
        else:
            # Inter-node control (no connection to reply on): route via network
            await self.channel.send_message(msg.sender_id, reply)

    async def _run_control_op(self, op: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if op == "ping":
            return {"node_id": self.node_id}

        if op == "get_stats":
            return {
                "node_id": self.node_id,
                "balance": self.balance,
                "available_balance": self.available_balance(),
                "pending_out": dict(self._pending_out),
                "channel": {
                    "sent": self.channel.sent_message_count,
                    "received": self.channel.received_message_count,
                },
                "ra": self.ricart_agrawala.stats,
                "tpc": self.tpc.stats,
                "snapshot": self.snapshot.stats,
                "faults": self.fault_injector.stats,
                "last_recovery": dict(self.last_recovery),
                "recovery_duration_ms": round(self.recovery_duration_ms, 3),
                "state_ra": self.ricart_agrawala.state.value,
            }

        if op == "set_fault":
            if "seed" in args:
                self.fault_injector.set_seed(int(args["seed"]))
            if "drop_rate" in args:
                self.fault_injector.set_drop_rate(float(args["drop_rate"]))
            if "delay_mean_ms" in args:
                self.fault_injector.set_delay(float(args["delay_mean_ms"]), float(args.get("delay_jitter_ms", 0.0)))
            if args.get("heal_partitions"):
                self.fault_injector.heal_all_partitions()
            for peer in args.get("partition_peers", []):
                self.fault_injector.partition(self.node_id, str(peer))
            return {"applied": True}

        if op == "request_cs":
            latency = await self.ricart_agrawala.request_cs()
            return {"latency_ms": round(latency * 1000.0, 3)}

        if op == "cs_cycle":
            """Full CS cycle in one atomic op (contention benchmarks: request,
            hold, release — otherwise release would only happen after all
            competing nodes acquired, deadlocking the losers)."""
            hold_sec = float(args.get("hold_sec", 0.01))
            t0 = asyncio.get_running_loop().time()
            await self.ricart_agrawala.request_cs()
            latency = asyncio.get_running_loop().time() - t0
            await asyncio.sleep(hold_sec)
            await self.ricart_agrawala.release_cs()
            return {"latency_ms": round(latency * 1000.0, 3)}

        if op == "release_cs":
            await self.ricart_agrawala.release_cs()
            return {"released": True}

        if op == "transfer":
            to = str(args["to"])
            amount = float(args["amount"])
            use_cs = bool(args.get("use_cs", False))
            tx_id = args.get("tx_id")
            t0 = asyncio.get_running_loop().time()
            success = await self.transfer_via_2pc(to, amount, use_cs=use_cs)
            latency_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
            return {"success": success, "latency_ms": round(latency_ms, 3)}

        if op == "transfer_then_crash":
            # Deterministic crash injection inside the coordinator protocol:
            #   before_decision: die after all votes, before the decision record.
            #   after_decision: die right after COMMITTED is durable, before apply
            #     and before GLOBAL_COMMIT propagation (exercises re-decision).
            to = str(args["to"])
            amount = float(args["amount"])
            tx_id = str(args["tx_id"])
            crash_point = str(args.get("crash_point", "before_decision"))

            def _die(_tx: str) -> None:
                self.crash()

            pre_hook = _die if crash_point == "before_decision" else None
            post_hook = _die if crash_point == "after_decision" else None

            async def _run() -> None:
                await self.tpc.execute_transaction(
                    [
                        {"from_node": self.node_id, "amount": amount},
                        {"to_node": to, "amount": amount},
                    ],
                    [self.node_id, to],
                    tx_id=tx_id,
                    pre_decision_hook=pre_hook,
                    post_decision_hook=post_hook,
                )

            asyncio.create_task(_run())
            # Give the event loop a moment to start the protocol; the process is
            # expected to die during execution, so this reply rarely arrives.
            await asyncio.sleep(0.05)
            return {"initiated": True, "tx_id": tx_id}

        if op == "snapshot":
            snapshot_id = str(args["snapshot_id"])
            t0 = asyncio.get_running_loop().time()
            snap = await self.snapshot.initiate_snapshot(snapshot_id)
            await asyncio.wait_for(snap.completion_event.wait(), timeout=float(args.get("timeout_sec", 30.0)))
            duration_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
            return {
                "duration_ms": round(duration_ms, 3),
                "markers_sent": self.snapshot.stats["markers_sent"],
                "markers_received": self.snapshot.stats["markers_received"],
            }

        if op == "audit":
            snapshot_id = str(args["snapshot_id"])
            if self.auditor is None:
                raise RuntimeError(f"Node {self.node_id} does not host the auditor")
            report = await self.auditor.wait_and_verify(snapshot_id, timeout_sec=float(args.get("timeout_sec", 5.0)))
            return {
                "is_consistent": report.is_consistent,
                "expected_total": report.expected_total,
                "computed_total": report.computed_total,
                "node_balances": report.node_balances,
                "channel_in_transit": report.channel_in_transit,
                "missing_nodes": report.missing_nodes,
            }

        if op == "load":
            target = str(args["target"])
            count = int(args.get("count", 1))
            rate = float(args.get("rate", 0.0))
            amount = float(args.get("amount", 10.0))
            queued = await self.start_background_load(target, count, rate, amount)
            return {"queued": queued}

        if op == "crash":
            self.crash()
            return {}  # unreachable

        if op == "stop":
            asyncio.create_task(self._delayed_stop())
            return {"stopping": True}

        raise ValueError(f"Unknown control op: {op}")

    async def _delayed_stop(self) -> None:
        await asyncio.sleep(0.1)
        await self.stop()
