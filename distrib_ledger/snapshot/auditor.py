"""Global Snapshot Auditor for verification of consistent cuts and invariant conservation."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from distrib_ledger.network.protocol import Message

logger = logging.getLogger(__name__)


@dataclass
class AuditReport:
    snapshot_id: str
    is_consistent: bool
    expected_total: float
    computed_total: float
    node_balances: Dict[str, float]
    channel_in_transit: Dict[str, float]
    missing_nodes: List[str] = field(default_factory=list)


class SnapshotAuditor:
    """Collects distributed snapshot pieces from nodes and verifies global invariants."""

    def __init__(self, all_nodes: List[str], expected_total_money: float):
        self.all_nodes = set(all_nodes)
        self.expected_total_money = expected_total_money
        # snapshot_id -> {node_id: report_payload}
        self.collected_reports: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._completion_events: Dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def handle_report(self, msg: Message) -> None:
        """Processes an incoming CL_SNAPSHOT_REPORT."""
        payload = msg.payload
        snap_id = payload["snapshot_id"]
        node_id = payload["node_id"]

        async with self._lock:
            if snap_id not in self.collected_reports:
                self.collected_reports[snap_id] = {}
                self._completion_events[snap_id] = asyncio.Event()

            self.collected_reports[snap_id][node_id] = payload
            logger.info("Auditor received snapshot report for %s from %s (%d/%d)", snap_id, node_id, len(self.collected_reports[snap_id]), len(self.all_nodes))

            if set(self.collected_reports[snap_id].keys()).issuperset(self.all_nodes):
                self._completion_events[snap_id].set()

    async def wait_and_verify(self, snapshot_id: str, timeout_sec: float = 5.0) -> AuditReport:
        """Awaits all reports for a snapshot and checks balance conservation."""
        event: Optional[asyncio.Event] = None
        async with self._lock:
            if snapshot_id not in self._completion_events:
                self._completion_events[snapshot_id] = asyncio.Event()
            event = self._completion_events[snapshot_id]

        try:
            async with asyncio.timeout(timeout_sec):
                await event.wait()
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning("Auditor timed out waiting for all snapshot reports for %s", snapshot_id)

        async with self._lock:
            reports = self.collected_reports.get(snapshot_id, {})

        node_balances: Dict[str, float] = {}
        channel_in_transit: Dict[str, float] = {}
        total_balance = 0.0
        total_transit = 0.0

        for nid in self.all_nodes:
            rep = reports.get(nid)
            if rep:
                bal = float(rep["local_state"].get("balance", 0.0))
                node_balances[nid] = bal
                total_balance += bal

                for ch, msgs in rep["channel_states"].items():
                    key = f"{ch}->{nid}"
                    ch_sum = sum(float(m.get("amount", 0.0)) for m in msgs)
                    channel_in_transit[key] = ch_sum
                    total_transit += ch_sum
            else:
                node_balances[nid] = 0.0

        computed_total = total_balance + total_transit
        # Allow tiny float precision margin
        is_consistent = abs(computed_total - self.expected_total_money) < 1e-4
        missing = list(self.all_nodes - set(reports.keys()))

        return AuditReport(
            snapshot_id=snapshot_id,
            is_consistent=is_consistent and len(missing) == 0,
            expected_total=self.expected_total_money,
            computed_total=computed_total,
            node_balances=node_balances,
            channel_in_transit=channel_in_transit,
            missing_nodes=missing,
        )
