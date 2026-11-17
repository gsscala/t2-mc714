"""Chandy-Lamport Distributed Snapshot Algorithm for Consistent Global Cuts."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType

logger = logging.getLogger(__name__)


@dataclass
class SnapshotState:
    snapshot_id: str
    local_state: Dict[str, Any]
    # channel_name (sender_id) -> list of in-transit recorded messages
    channel_states: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    # Channels where recording is still active
    recording_channels: Set[str] = field(default_factory=set)
    # Channels from which a marker has been received
    markers_received: Set[str] = field(default_factory=set)
    is_complete: bool = False
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)


class ChandyLamport:
    """Implements Chandy-Lamport distributed snapshot over FIFO channels.
    
    Guarantees:
      - Safety: Records a consistent cut without pausing normal transaction processing.
      - Invariant: Conserves total money across node balances and in-transit messages.
      - Liveness: Completes once markers traverse all incoming channels.
    """

    def __init__(
        self,
        node_id: str,
        channel: NetworkChannel,
        get_local_state_cb: Callable[[], Dict[str, Any]],
        peers: List[str],
        auditor_id: Optional[str] = None,
    ):
        self.node_id = node_id
        self.channel = channel
        self.get_local_state_cb = get_local_state_cb
        self.peers = [p for p in peers if p != node_id]
        self.auditor_id = auditor_id

        # snapshot_id -> SnapshotState
        self.snapshots: Dict[str, SnapshotState] = {}
        self._lock = asyncio.Lock()
        self._markers_sent = 0
        self._markers_received = 0

        # Marker ordering gate: application senders wait until every pending
        # snapshot marker for the destination has been written to the socket.
        # Without it, a transfer sent by this node right after its state capture
        # could overtake the forwarded marker on the wire (per-peer lock race),
        # and the receiver would both apply the transfer and record it as
        # in-transit — double counting the amount in the audited cut.
        self._pending_marker_dests: Dict[str, int] = {}
        self._marker_gate = asyncio.Condition()

    async def initiate_snapshot(self, snapshot_id: str) -> SnapshotState:
        """Initiates a new global snapshot from this node."""
        # Reserve the marker gate for every destination before capturing state,
        # mirroring handle_marker: application sends issued after this point wait
        # until the initiator's markers are on the wire.
        async with self._marker_gate:
            for p in self.peers:
                self._pending_marker_dests[p] = self._pending_marker_dests.get(p, 0) + 1

        try:
            async with self._lock:
                local_state = self.get_local_state_cb()
                snap = SnapshotState(
                    snapshot_id=snapshot_id,
                    local_state=dict(local_state),
                    channel_states={p: [] for p in self.peers},
                    recording_channels=set(self.peers),
                    markers_received=set(),
                )
                self.snapshots[snapshot_id] = snap
        except BaseException:
            async with self._marker_gate:
                for p in self.peers:
                    self._pending_marker_dests[p] = max(0, self._pending_marker_dests.get(p, 0) - 1)
                    self._marker_gate.notify_all()
            raise

        logger.info("Node %s INITIATED snapshot %s with local state %s", self.node_id, snapshot_id, local_state)

        # Marker-sending rule: send marker along all outgoing channels
        marker = Message(
            msg_type=MessageType.CL_MARKER,
            sender_id=self.node_id,
            timestamp=0,
            payload={"snapshot_id": snapshot_id},
        )
        for p in self.peers:
            try:
                await self.channel.send_message(p, marker)
                self._markers_sent += 1
            finally:
                async with self._marker_gate:
                    self._pending_marker_dests[p] = max(0, self._pending_marker_dests.get(p, 0) - 1)
                    self._marker_gate.notify_all()

        # Check if 0 peers
        if not self.peers:
            snap.is_complete = True
            snap.completion_event.set()

        return snap

    async def before_app_send(self, dest_id: str) -> None:
        """Blocks while a snapshot marker for dest_id is still pending on the wire.

        Application senders MUST call this before writing an application message,
        so that a message sent after this node recorded its state never overtakes
        the marker on the same channel.
        """
        async with self._marker_gate:
            while self._pending_marker_dests.get(dest_id, 0) > 0:
                await self._marker_gate.wait()

    async def handle_marker(self, msg: Message) -> None:
        """Processes an incoming CL_MARKER according to Chandy-Lamport rules."""
        snapshot_id = msg.payload["snapshot_id"]
        sender = msg.sender_id
        self._markers_received += 1

        send_markers_to: List[str] = []

        # Reserve the marker gate before capturing state: any application send
        # that starts from here on waits until this node's markers are out.
        async with self._marker_gate:
            for p in self.peers:
                self._pending_marker_dests[p] = self._pending_marker_dests.get(p, 0) + 1

        try:
            async with self._lock:
                if snapshot_id not in self.snapshots:
                    # First time seeing this snapshot: record local state immediately
                    local_state = self.get_local_state_cb()
                    snap = SnapshotState(
                        snapshot_id=snapshot_id,
                        local_state=dict(local_state),
                        channel_states={p: [] for p in self.peers},
                        recording_channels=set(self.peers) - {sender},
                        markers_received={sender},
                    )
                    self.snapshots[snapshot_id] = snap
                    logger.info(
                        "Node %s FIRST marker for snapshot %s from %s. Recorded local state: %s",
                        self.node_id,
                        snapshot_id,
                        sender,
                        local_state,
                    )
                    # Must propagate marker to all other peers
                    send_markers_to = list(self.peers)
                else:
                    snap = self.snapshots[snapshot_id]
                    snap.markers_received.add(sender)
                    if sender in snap.recording_channels:
                        snap.recording_channels.remove(sender)
                        logger.debug(
                            "Node %s STOPPED recording on channel from %s for snapshot %s (recorded %d in-flight messages)",
                            self.node_id,
                            sender,
                            snapshot_id,
                            len(snap.channel_states.get(sender, [])),
                        )

                # Check if all incoming channels have received markers
                if set(self.peers).issubset(snap.markers_received):
                    snap.is_complete = True
                    snap.completion_event.set()
                    logger.info("Node %s COMPLETED snapshot collection for %s", self.node_id, snapshot_id)
        finally:
            # The capture is done; release the gate reservation for channels that
            # will not receive a forwarded marker (e.g. the initiator, which
            # sends its own markers through initiate_snapshot instead).
            if not send_markers_to:
                async with self._marker_gate:
                    for p in self.peers:
                        self._pending_marker_dests[p] = max(0, self._pending_marker_dests.get(p, 0) - 1)
                        self._marker_gate.notify_all()

        # Propagate markers outside lock, then release each destination's gate
        if send_markers_to:
            marker = Message(
                msg_type=MessageType.CL_MARKER,
                sender_id=self.node_id,
                timestamp=0,
                payload={"snapshot_id": snapshot_id},
            )
            for p in send_markers_to:
                try:
                    await self.channel.send_message(p, marker)
                    self._markers_sent += 1
                finally:
                    async with self._marker_gate:
                        self._pending_marker_dests[p] = max(0, self._pending_marker_dests.get(p, 0) - 1)
                        self._marker_gate.notify_all()

        # If complete and auditor configured, forward snapshot report
        if snap.is_complete and self.auditor_id:
            await self._report_to_auditor(snap)

    async def intercept_incoming_app_message(self, sender_id: str, msg_payload: Dict[str, Any]) -> None:
        """Records application messages arriving on a channel while channel recording is active."""
        async with self._lock:
            for snap in self.snapshots.values():
                if not snap.is_complete and sender_id in snap.recording_channels:
                    snap.channel_states[sender_id].append(dict(msg_payload))
                    logger.debug(
                        "Node %s RECORDED in-transit message from %s during snapshot %s: %s",
                        self.node_id,
                        sender_id,
                        snap.snapshot_id,
                        msg_payload,
                    )

    async def _report_to_auditor(self, snap: SnapshotState) -> None:
        """Sends collected snapshot report to auditor node."""
        report = Message(
            msg_type=MessageType.CL_SNAPSHOT_REPORT,
            sender_id=self.node_id,
            timestamp=0,
            payload={
                "snapshot_id": snap.snapshot_id,
                "node_id": self.node_id,
                "local_state": snap.local_state,
                "channel_states": snap.channel_states,
            },
        )
        if self.auditor_id == self.node_id:
            for handler in self.channel._incoming_handlers:
                await handler(report)
        else:
            await self.channel.send_message(self.auditor_id, report)

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "snapshots_tracked": len(self.snapshots),
            "markers_sent": self._markers_sent,
            "markers_received": self._markers_received,
        }
