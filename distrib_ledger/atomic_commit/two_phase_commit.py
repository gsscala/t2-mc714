"""Two-Phase Commit (2PC) Protocol with Write-Ahead Logging and Cooperative Recovery."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from distrib_ledger.atomic_commit.wal import WalRecordType, WriteAheadLog
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType

logger = logging.getLogger(__name__)


class TxStatus(str, Enum):
    INIT = "INIT"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


@dataclass
class TransactionContext:
    tx_id: str
    operations: List[Dict[str, Any]]
    participants: List[str]
    status: TxStatus = TxStatus.INIT
    votes: Dict[str, str] = field(default_factory=dict)
    acks: Set[str] = field(default_factory=set)
    decision_event: asyncio.Event = field(default_factory=asyncio.Event)


class TwoPhaseCommit:
    """Implements 2PC protocol for distributed transactional balance transfers.

    Guarantees:
      - Safety: Atomicity across all nodes (all commit or all abort). A decision is
        applied at most once per node: the COMMITTED record is written before the
        local apply, and an APPLIED record makes recovery re-application idempotent.
      - Durability: Crash-recovery via Write-Ahead Log with fsync before every vote
        or decision is communicated.
      - Liveness: In-doubt participants resolve through coordinator retry or peer
        queries; they never unilaterally abort a transaction whose outcome may have
        been decided by the coordinator (blocking is the documented 2PC trade-off).
    """

    def __init__(
        self,
        node_id: str,
        channel: NetworkChannel,
        wal: WriteAheadLog,
        apply_commit_cb: Optional[Callable[[str, List[Dict[str, Any]]], bool]] = None,
        validate_prepare_cb: Optional[Callable[[str, List[Dict[str, Any]]], bool]] = None,
        abort_rollback_cb: Optional[Callable[[str], None]] = None,
        vote_timeout_sec: float = 3.0,
        decision_retransmit_rounds: int = 5,
        decision_retransmit_interval_sec: float = 0.3,
    ):
        self.node_id = node_id
        self.channel = channel
        self.wal = wal
        self.apply_commit_cb = apply_commit_cb
        self.validate_prepare_cb = validate_prepare_cb
        self.abort_rollback_cb = abort_rollback_cb
        self.vote_timeout_sec = vote_timeout_sec
        self.decision_retransmit_rounds = decision_retransmit_rounds
        self.decision_retransmit_interval_sec = decision_retransmit_interval_sec

        # Active transactions: tx_id -> TransactionContext
        self.active_transactions: Dict[str, TransactionContext] = {}
        # Completed transactions: tx_id -> TxStatus
        self.completed_transactions: Dict[str, TxStatus] = {}
        # Vote memory: tx_id -> vote sent (duplicate PREPAREs must not re-validate)
        self._votes_sent: Dict[str, str] = {}

        self._lock = asyncio.Lock()
        self._stats = {
            "transactions_started": 0,
            "transactions_committed": 0,
            "transactions_aborted": 0,
        }

    async def execute_transaction(
        self,
        operations: List[Dict[str, Any]],
        participants: List[str],
        tx_id: Optional[str] = None,
        pre_decision_hook: Optional[Callable[[str], None]] = None,
        post_decision_hook: Optional[Callable[[str], None]] = None,
    ) -> bool:
        """Coordinator initiates and manages a distributed transaction to completion.

        Args:
            tx_id: Optional externally supplied transaction id (used by tests and
                crash-injection drivers that must refer to the transaction later).
            pre_decision_hook: Called with tx_id after all votes are collected and
                before the coordinator decides (used for deterministic crash tests).
            post_decision_hook: Called with tx_id immediately after the COMMITTED
                record is durably logged and before GLOBAL_COMMIT is propagated.
        """
        tx_id = tx_id or f"tx_{uuid.uuid4().hex[:10]}"
        ctx = TransactionContext(
            tx_id=tx_id,
            operations=operations,
            participants=list(participants),
            status=TxStatus.PREPARING,
        )

        async with self._lock:
            self.active_transactions[tx_id] = ctx
            self._stats["transactions_started"] += 1
            self.wal.append(
                WalRecordType.START_2PC,
                tx_id,
                {"operations": operations, "participants": participants},
            )

        logger.info("Coordinator %s START_2PC for tx %s (participants: %s)", self.node_id, tx_id, participants)

        # Phase 1: Send PREPARE to all participants
        prepare_msg = Message(
            msg_type=MessageType.TPC_PREPARE,
            sender_id=self.node_id,
            timestamp=0,
            payload={
                "tx_id": tx_id,
                "operations": operations,
                "participants": participants,
            },
        )

        for p in participants:
            if p == self.node_id:
                # Local vote (validate_prepare_cb reserves funds atomically)
                can_commit = True
                if self.validate_prepare_cb:
                    can_commit = self.validate_prepare_cb(tx_id, operations)
                vote = "COMMIT" if can_commit else "ABORT"
                self._votes_sent[tx_id] = vote
                ctx.votes[self.node_id] = vote
                logger.info(
                    "Coordinator %s reserved funds and voted %s for tx %s", self.node_id, vote, tx_id
                )
            else:
                await self.channel.send_message(p, prepare_msg)

        # Wait for all votes or timeout
        needed_votes = len(participants)
        try:
            async with asyncio.timeout(self.vote_timeout_sec):
                while len(ctx.votes) < needed_votes:
                    await ctx.decision_event.wait()
                    ctx.decision_event.clear()
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning("Coordinator %s timed out waiting for votes on tx %s (%d/%d received)", self.node_id, tx_id, len(ctx.votes), needed_votes)

        if pre_decision_hook is not None:
            pre_decision_hook(tx_id)

        # Phase 2: Decision
        all_committed = (
            len(ctx.votes) == needed_votes
            and all(v == "COMMIT" for v in ctx.votes.values())
        )

        if all_committed:
            ctx.status = TxStatus.COMMITTED
            self.wal.append(WalRecordType.COMMITTED, tx_id, {"operations": operations, "participants": participants})
            self.completed_transactions[tx_id] = TxStatus.COMMITTED
            self._stats["transactions_committed"] += 1

            if post_decision_hook is not None:
                post_decision_hook(tx_id)

            # Apply locally and mark the apply as durable (idempotent recovery)
            self._apply_local_commit(tx_id, operations)

            commit_msg = Message(
                msg_type=MessageType.TPC_GLOBAL_COMMIT,
                sender_id=self.node_id,
                timestamp=0,
                payload={"tx_id": tx_id, "operations": operations},
            )

            # Disseminate the decision, retransmitting to participants that have
            # not ACKed (recovers a dropped GLOBAL_COMMIT without diverging).
            await self._disseminate_decision(
                lambda: commit_msg, participants, ctx, self.vote_timeout_sec
            )

            self.active_transactions.pop(tx_id, None)
            logger.info("Coordinator %s GLOBAL_COMMIT completed for tx %s", self.node_id, tx_id)
            return True
        else:
            ctx.status = TxStatus.ABORTED
            self.wal.append(WalRecordType.ABORTED, tx_id, {"reason": "Vote abort or timeout"})
            self.completed_transactions[tx_id] = TxStatus.ABORTED
            self._stats["transactions_aborted"] += 1

            # Release any local reservation taken during the local vote
            if self.node_id in participants and self.abort_rollback_cb:
                self.abort_rollback_cb(tx_id)

            abort_msg = Message(
                msg_type=MessageType.TPC_GLOBAL_ABORT,
                sender_id=self.node_id,
                timestamp=0,
                payload={"tx_id": tx_id, "reason": "Vote abort or timeout"},
            )
            await self._disseminate_decision(
                lambda: abort_msg, participants, ctx, self.vote_timeout_sec
            )

            self.active_transactions.pop(tx_id, None)
            logger.warning("Coordinator %s GLOBAL_ABORT for tx %s", self.node_id, tx_id)
            return False

    async def _disseminate_decision(
        self,
        build_msg: Callable[[], Message],
        participants: List[str],
        ctx: TransactionContext,
        first_wait_sec: float,
    ) -> None:
        """Sends the coordinator's decision and retransmits it until every remote
        participant ACKs or the retransmission budget is exhausted.

        The decision itself never changes; retransmission only accelerates the
        participants' convergence and is safe under duplicates (participants
        guard against re-applying).
        """
        remotes = [p for p in participants if p != self.node_id]
        if not remotes:
            return

        for attempt in range(self.decision_retransmit_rounds + 1):
            missing = [p for p in remotes if p not in ctx.acks]
            if not missing:
                return
            for p in missing:
                await self.channel.send_message(p, build_msg())
            if attempt == self.decision_retransmit_rounds:
                break
            wait_sec = first_wait_sec if attempt == 0 else self.decision_retransmit_interval_sec
            try:
                async with asyncio.timeout(wait_sec):
                    while not set(remotes).issubset(ctx.acks):
                        await ctx.decision_event.wait()
                        ctx.decision_event.clear()
            except (TimeoutError, asyncio.TimeoutError):
                continue

        still_missing = [p for p in remotes if p not in ctx.acks]
        if still_missing:
            logger.warning(
                "Coordinator %s: participants %s did not ACK tx %s; they will resolve via query on recovery",
                self.node_id, still_missing, ctx.tx_id,
            )

    def _apply_local_commit(self, tx_id: str, operations: List[Dict[str, Any]]) -> None:
        """Applies committed operations locally and durably marks the apply.

        The APPLIED record makes crash-recovery re-application idempotent: a node
        that finds COMMITTED without APPLIED in its log replays the operations
        exactly once.
        """
        if self.apply_commit_cb:
            self.apply_commit_cb(tx_id, operations)
        self.wal.append(WalRecordType.APPLIED, tx_id, {})

    async def handle_prepare(self, msg: Message) -> None:
        """Participant handles PREPARE request from coordinator.

        Duplicate PREPAREs are answered with the originally recorded vote, never
        re-validated against a balance that may have changed in between.
        """
        tx_id = msg.payload["tx_id"]
        operations = msg.payload["operations"]
        coordinator = msg.sender_id

        if tx_id in self._votes_sent:
            vote = self._votes_sent[tx_id]
            logger.info("Participant %s re-sending stored vote %s for duplicate PREPARE of tx %s", self.node_id, vote, tx_id)
        else:
            can_commit = True
            if self.validate_prepare_cb:
                can_commit = self.validate_prepare_cb(tx_id, operations)
            vote = "COMMIT" if can_commit else "ABORT"
            self._votes_sent[tx_id] = vote

        if vote == "COMMIT":
            self.wal.append(WalRecordType.PREPARED, tx_id, {"operations": operations, "coordinator": coordinator})
            logger.info(
                "Participant %s wrote PREPARED to WAL (fsync) for tx %s", self.node_id, tx_id
            )
            vote_msg = Message(
                msg_type=MessageType.TPC_VOTE_COMMIT,
                sender_id=self.node_id,
                timestamp=0,
                payload={"tx_id": tx_id},
            )
            await self.channel.send_message(coordinator, vote_msg)
            logger.info("Participant %s voted COMMIT for tx %s", self.node_id, tx_id)
        else:
            self.wal.append(WalRecordType.ABORTED, tx_id, {"reason": "Validation failed", "coordinator": coordinator})
            vote_msg = Message(
                msg_type=MessageType.TPC_VOTE_ABORT,
                sender_id=self.node_id,
                timestamp=0,
                payload={"tx_id": tx_id, "reason": "Validation failed"},
            )
            await self.channel.send_message(coordinator, vote_msg)
            logger.warning("Participant %s voted ABORT for tx %s", self.node_id, tx_id)

    async def handle_vote_commit(self, msg: Message) -> None:
        """Coordinator records a COMMIT vote."""
        tx_id = msg.payload["tx_id"]
        if tx_id in self.active_transactions:
            ctx = self.active_transactions[tx_id]
            ctx.votes[msg.sender_id] = "COMMIT"
            ctx.decision_event.set()

    async def handle_vote_abort(self, msg: Message) -> None:
        """Coordinator records an ABORT vote."""
        tx_id = msg.payload["tx_id"]
        if tx_id in self.active_transactions:
            ctx = self.active_transactions[tx_id]
            ctx.votes[msg.sender_id] = "ABORT"
            ctx.decision_event.set()

    async def handle_global_commit(self, msg: Message) -> None:
        """Participant applies global commit (idempotent under duplicates)."""
        tx_id = msg.payload["tx_id"]
        operations = msg.payload.get("operations", [])

        if self.completed_transactions.get(tx_id) == TxStatus.COMMITTED:
            # Duplicate decision (e.g. coordinator re-propagation after recovery)
            logger.info("Participant %s ignoring duplicate GLOBAL_COMMIT for tx %s", self.node_id, tx_id)
        else:
            self.wal.append(WalRecordType.COMMITTED, tx_id, {"operations": operations})
            self.completed_transactions[tx_id] = TxStatus.COMMITTED
            self._apply_local_commit(tx_id, operations)

        ack = Message(
            msg_type=MessageType.TPC_ACK,
            sender_id=self.node_id,
            timestamp=0,
            payload={"tx_id": tx_id},
        )
        await self.channel.send_message(msg.sender_id, ack)
        logger.info("Participant %s executed GLOBAL_COMMIT for tx %s", self.node_id, tx_id)

    async def handle_global_abort(self, msg: Message) -> None:
        """Participant handles global abort (idempotent under duplicates)."""
        tx_id = msg.payload["tx_id"]

        if self.completed_transactions.get(tx_id) == TxStatus.ABORTED:
            logger.info("Participant %s ignoring duplicate GLOBAL_ABORT for tx %s", self.node_id, tx_id)
        else:
            self.wal.append(WalRecordType.ABORTED, tx_id, {"reason": "Coordinator decision"})
            self.completed_transactions[tx_id] = TxStatus.ABORTED
            if self.abort_rollback_cb:
                self.abort_rollback_cb(tx_id)

        ack = Message(
            msg_type=MessageType.TPC_ACK,
            sender_id=self.node_id,
            timestamp=0,
            payload={"tx_id": tx_id},
        )
        await self.channel.send_message(msg.sender_id, ack)
        logger.info("Participant %s executed GLOBAL_ABORT for tx %s", self.node_id, tx_id)

    async def handle_ack(self, msg: Message) -> None:
        """Coordinator collects acknowledgement."""
        tx_id = msg.payload["tx_id"]
        if tx_id in self.active_transactions:
            self.active_transactions[tx_id].acks.add(msg.sender_id)
            self.active_transactions[tx_id].decision_event.set()

    async def handle_query_status(self, msg: Message) -> None:
        """Responds to peer termination protocol queries."""
        tx_id = msg.payload["tx_id"]
        status = self.completed_transactions.get(tx_id, TxStatus.INIT).value
        reply = Message(
            msg_type=MessageType.TPC_STATUS_REPLY,
            sender_id=self.node_id,
            timestamp=0,
            payload={"tx_id": tx_id, "status": status},
        )
        await self.channel.send_message(msg.sender_id, reply)

    @property
    def stats(self) -> Dict[str, int]:
        return dict(self._stats)
