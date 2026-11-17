"""Crash-Recovery Manager and Cooperative Termination Protocol for 2PC."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Dict, List, Optional

from distrib_ledger.atomic_commit.two_phase_commit import TwoPhaseCommit, TxStatus
from distrib_ledger.atomic_commit.wal import WalRecordType, WriteAheadLog
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType

logger = logging.getLogger(__name__)


class RecoveryManager:
    """Scans WAL on node restart, resolves in-doubt transactions, and restores consistency.

    Recovery rules (Presumed Abort):
      - Coordinator with START_2PC and no decision: abort unilaterally and notify
        participants (safe because no COMMITTED record was ever logged).
      - Coordinator with COMMITTED: re-apply locally if needed (idempotent via the
        APPLIED record) and re-propagate GLOBAL_COMMIT to stranded participants.
      - Participant with PREPARED (in-doubt): query the coordinator and peers.
        Adopt COMMIT or ABORT from an authoritative answer, but never abort
        unilaterally when nobody knows the outcome — the transaction remains
        in doubt and is retried on the next recovery. Unilateral abort here could
        diverge from a coordinator that durably decided COMMIT before crashing.
    """

    def __init__(
        self,
        node_id: str,
        tpc: TwoPhaseCommit,
        wal: WriteAheadLog,
        channel: NetworkChannel,
        peers: List[str],
        resolve_rounds: int = 3,
        resolve_query_timeout_sec: float = 1.5,
        resolve_round_pause_sec: float = 0.3,
    ):
        self.node_id = node_id
        self.tpc = tpc
        self.wal = wal
        self.channel = channel
        self.peers = peers
        self.resolve_rounds = resolve_rounds
        self.resolve_query_timeout_sec = resolve_query_timeout_sec
        self.resolve_round_pause_sec = resolve_round_pause_sec

    async def recover(self) -> Dict[str, str]:
        """Performs full crash recovery replay from WAL.

        Returns tx_id -> final status, where the status may be COMMITTED, ABORTED
        or IN_DOUBT (kept in the PREPARED state for a later recovery round).
        """
        logger.info("Node %s starting crash recovery from WAL", self.node_id)
        recovered_txs = self.wal.recover()
        results: Dict[str, str] = {}

        for tx_id, tx_info in recovered_txs.items():
            status = tx_info["status"]
            data = tx_info.get("data", {})

            if status == WalRecordType.COMMITTED.value:
                self._recover_committed(tx_id, tx_info, data)
                results[tx_id] = "COMMITTED"

            elif status == WalRecordType.APPLIED.value:
                # Apply already durably recorded before the crash: nothing to redo.
                self.tpc.completed_transactions[tx_id] = TxStatus.COMMITTED
                results[tx_id] = "COMMITTED"
                logger.info("Node %s recovered already-APPLIED tx %s", self.node_id, tx_id)

            elif status == WalRecordType.ABORTED.value:
                self.tpc.completed_transactions[tx_id] = TxStatus.ABORTED
                results[tx_id] = "ABORTED"
                logger.info("Node %s recovered ABORTED tx %s", self.node_id, tx_id)

            elif status == WalRecordType.START_2PC.value:
                # Coordinator crashed before deciding: Presumed Abort.
                logger.warning(
                    "Node %s was coordinator for in-flight tx %s. Presumed Abort applied.",
                    self.node_id,
                    tx_id,
                )
                self.wal.append(WalRecordType.ABORTED, tx_id, {"reason": "Coordinator crash before decision"})
                self.tpc.completed_transactions[tx_id] = TxStatus.ABORTED
                results[tx_id] = "ABORTED"
                # Notify participants best-effort; any participant still in doubt
                # will query this coordinator and learn the ABORT decision.
                participants = data.get("participants", self.peers)
                abort_msg = Message(
                    msg_type=MessageType.TPC_GLOBAL_ABORT,
                    sender_id=self.node_id,
                    timestamp=0,
                    payload={"tx_id": tx_id, "reason": "Coordinator crash recovery"},
                )
                for p in participants:
                    if p != self.node_id:
                        await self.channel.send_message(p, abort_msg)

            elif status == WalRecordType.PREPARED.value:
                # In-doubt participant: query coordinator and peers.
                logger.warning("Node %s recovered in-doubt PREPARED tx %s. Initiating cooperative query.", self.node_id, tx_id)
                coordinator = data.get("coordinator")
                decided = await self._resolve_in_doubt(tx_id, coordinator, data.get("operations", []))
                results[tx_id] = decided
                if decided == "COMMITTED":
                    self._finalize_committed(tx_id, data.get("operations", []))
                elif decided == "ABORTED":
                    self.wal.append(WalRecordType.ABORTED, tx_id, {"reason": "Resolved via cooperative termination"})
                    self.tpc.completed_transactions[tx_id] = TxStatus.ABORTED
                # IN_DOUBT: keep the PREPARED record untouched for the next recovery.

        logger.info("Node %s finished recovery: %d transactions processed", self.node_id, len(results))
        return results

    def _recover_committed(self, tx_id: str, tx_info: Dict, data: Dict) -> None:
        """Coordinator or participant restart with a COMMITTED record.

        Re-applies the operations exactly once (idempotency via the APPLIED
        record) and, on the coordinator, re-propagates the decision to
        participants that may have crashed before receiving it.
        """
        operations = data.get("operations", [])
        applied = any(r.record_type == WalRecordType.APPLIED for r in tx_info["records"])

        if applied:
            logger.info("Node %s recovered COMMITTED tx %s (apply already durable)", self.node_id, tx_id)
        else:
            logger.warning("Node %s recovered COMMITTED tx %s without APPLIED record; re-applying locally", self.node_id, tx_id)
            self._finalize_committed(tx_id, operations)

        self.tpc.completed_transactions[tx_id] = TxStatus.COMMITTED

        # Coordinator side: re-propagate so stranded participants can resolve.
        participants = data.get("participants", [])
        if participants:
            commit_msg = Message(
                msg_type=MessageType.TPC_GLOBAL_COMMIT,
                sender_id=self.node_id,
                timestamp=0,
                payload={"tx_id": tx_id, "operations": operations},
            )
            for p in participants:
                if p != self.node_id:
                    asyncio.create_task(self.channel.send_message(p, commit_msg))

    def _finalize_committed(self, tx_id: str, operations: List) -> None:
        """Applies operations and durably records APPLIED (recovery path)."""
        if self.tpc.apply_commit_cb:
            self.tpc.apply_commit_cb(tx_id, operations)
        self.wal.append(WalRecordType.APPLIED, tx_id, {})

    async def _resolve_in_doubt(
        self, tx_id: str, coordinator: Optional[str], operations: list
    ) -> str:
        """Queries coordinator and peer participants to learn the final decision.

        COMMIT and ABORT are both sound to adopt from any peer: a peer only
        reports COMMITTED after a GLOBAL_COMMIT and only reports ABORTED after a
        decision (or its own ABORT vote, which forces a global abort). When no
        authoritative answer is found within the round budget the transaction is
        left in doubt instead of being aborted.
        """
        targets = [coordinator] if coordinator and coordinator != self.node_id else []
        targets.extend([p for p in self.peers if p != self.node_id and p != coordinator])

        for round_num in range(1, self.resolve_rounds + 1):
            decision = await self._query_round(tx_id, targets)
            if decision in ("COMMITTED", "ABORTED"):
                logger.info("Node %s resolved in-doubt tx %s as %s (round %d)", self.node_id, tx_id, decision, round_num)
                return decision
            if round_num < self.resolve_rounds:
                await asyncio.sleep(self.resolve_round_pause_sec)

        logger.warning(
            "Node %s could not resolve in-doubt tx %s after %d rounds; remaining IN_DOUBT",
            self.node_id, tx_id, self.resolve_rounds,
        )
        return "IN_DOUBT"

    async def _query_round(self, tx_id: str, targets: List[str]) -> Optional[str]:
        """One round of status queries. Returns the first authoritative answer."""
        for target in targets:
            if not target:
                continue
            query_msg = Message(
                msg_type=MessageType.TPC_QUERY_STATUS,
                sender_id=self.node_id,
                timestamp=0,
                payload={"tx_id": tx_id},
            )
            future: asyncio.Future[str] = asyncio.get_running_loop().create_future()

            async def status_listener(msg: Message, fut: asyncio.Future = future) -> None:
                if msg.msg_type == MessageType.TPC_STATUS_REPLY and msg.payload.get("tx_id") == tx_id:
                    if not fut.done():
                        fut.set_result(msg.payload.get("status", "INIT"))

            unsubscribe = self.channel.register_handler(status_listener)
            try:
                await self.channel.send_message(target, query_msg)
                try:
                    async with asyncio.timeout(self.resolve_query_timeout_sec):
                        status = await future
                        if status in ("COMMITTED", "ABORTED"):
                            return status
                except (TimeoutError, asyncio.TimeoutError):
                    continue
            finally:
                unsubscribe()

        return None
