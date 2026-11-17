"""Ricart-Agrawala Distributed Mutual Exclusion Algorithm."""

from __future__ import annotations

import asyncio
import logging
import uuid
from enum import Enum
from typing import Dict, List, Optional, Set

from distrib_ledger.mutual_exclusion.lamport_clock import LamportClock
from distrib_ledger.network.channel import NetworkChannel
from distrib_ledger.network.protocol import Message, MessageType

logger = logging.getLogger(__name__)


class NodeState(str, Enum):
    RELEASED = "RELEASED"
    WANTED = "WANTED"
    HELD = "HELD"


class RicartAgrawala:
    """Implements Ricart-Agrawala distributed mutual exclusion (1981).

    Guarantees:
      - Safety: At most one node in the critical section at any time. A node only
        enters the CS after collecting all N-1 replies — it never assumes a slow
        or silent peer has crashed, because that would break safety under message
        loss or long delays.
      - Liveness: Starvation-free and deadlock-free via the total order
        (Lamport timestamp, node_id). Under message loss, REQUESTs are
        retransmitted idempotently (duplicates are deduplicated by the deferred
        queue), so a lost request or reply only delays, never blocks forever on a
        live peer.
      - Complexity: Exactly 2*(N-1) messages per CS entry in a fault-free run;
        retransmissions are counted separately.
      - Known limitation (documented): with static membership, a permanently
        crashed peer blocks CS entry — the trade-off that preserves safety.
    """

    def __init__(
        self,
        node_id: str,
        channel: NetworkChannel,
        clock: Optional[LamportClock] = None,
        peers: Optional[List[str]] = None,
        retransmit_interval_sec: float = 0.5,
    ):
        self.node_id = node_id
        self.channel = channel
        self.clock = clock or LamportClock(node_id)
        self.peers = peers if peers is not None else [p for p in channel.peer_addresses.keys() if p != node_id]
        self.retransmit_interval_sec = retransmit_interval_sec

        self.state = NodeState.RELEASED
        self.request_time = 0
        self.request_id = ""
        self.replies_received: Set[str] = set()
        # sender -> req_id of the deferred request (deduplicates retransmissions)
        self.deferred_queue: Dict[str, str] = {}

        self._cs_lock = asyncio.Lock()
        self._reply_event = asyncio.Event()
        self._total_cs_entries = 0
        self._total_requests_sent = 0
        self._total_replies_sent = 0
        self._total_retransmits_sent = 0

    async def request_cs(self) -> float:
        """Requests entry into Critical Section. Blocks until permission granted.

        Returns:
            Wait time in seconds to acquire the CS.
        """
        async with self._cs_lock:
            start_time = asyncio.get_running_loop().time()
            self.state = NodeState.WANTED
            self.request_time = await self.clock.tick()
            self.request_id = str(uuid.uuid4())
            self.replies_received.clear()

            needed_replies = len(self.peers)
            if needed_replies == 0:
                self.state = NodeState.HELD
                self._total_cs_entries += 1
                return 0.0

            req_msg = Message(
                msg_type=MessageType.RA_REQUEST,
                sender_id=self.node_id,
                timestamp=self.request_time,
                payload={"req_id": self.request_id},
            )

            # Broadcast request to all N-1 peers
            for peer in self.peers:
                await self.channel.send_message(peer, req_msg)
                self._total_requests_sent += 1

            # Wait for ALL replies. Lost messages are recovered by periodic
            # idempotent retransmission; the node never enters the CS with
            # outstanding replies (that would sacrifice safety).
            while len(self.replies_received) < needed_replies:
                missing = [p for p in self.peers if p not in self.replies_received]
                try:
                    async with asyncio.timeout(self.retransmit_interval_sec):
                        await self._reply_event.wait()
                        self._reply_event.clear()
                except (TimeoutError, asyncio.TimeoutError):
                    for peer in missing:
                        logger.debug(
                            "Node %s retransmitting REQUEST to %s (%d replies pending)",
                            self.node_id, peer, needed_replies - len(self.replies_received),
                        )
                        await self.channel.send_message(peer, req_msg)
                        self._total_retransmits_sent += 1

            self.state = NodeState.HELD
            self._total_cs_entries += 1
            acquired_time = asyncio.get_running_loop().time()
            latency = acquired_time - start_time
            logger.info("Node %s ENTERED critical section (latency: %.4fs)", self.node_id, latency)
            return latency

    async def release_cs(self) -> None:
        """Exits Critical Section and flushes deferred reply queue."""
        async with self._cs_lock:
            if self.state != NodeState.HELD:
                logger.warning("Node %s called release_cs but state is %s", self.node_id, self.state)
                return

            self.state = NodeState.RELEASED
            logger.info("Node %s EXITED critical section. Flushing %d deferred replies.", self.node_id, len(self.deferred_queue))

            deferred = dict(self.deferred_queue)
            self.deferred_queue.clear()

            for peer, req_id in deferred.items():
                t = await self.clock.tick()
                reply_msg = Message(
                    msg_type=MessageType.RA_REPLY,
                    sender_id=self.node_id,
                    timestamp=t,
                    payload={"receiver_id": peer, "req_id": req_id},
                )
                await self.channel.send_message(peer, reply_msg)
                self._total_replies_sent += 1

    async def handle_request(self, msg: Message) -> None:
        """Processes an incoming RA_REQUEST (idempotent under retransmission)."""
        sender = msg.sender_id
        incoming_time = msg.timestamp
        req_id = msg.payload.get("req_id", "")

        await self.clock.update(incoming_time)

        defer = False
        if self.state == NodeState.HELD:
            defer = True
        elif self.state == NodeState.WANTED:
            # Check priority: our request has priority if our timestamp is smaller,
            # or if timestamps tie, our node_id is lexicographically smaller.
            our_priority = (self.request_time < incoming_time) or (
                self.request_time == incoming_time and self.node_id < sender
            )
            if our_priority:
                defer = True

        if defer:
            if sender not in self.deferred_queue:
                self.deferred_queue[sender] = req_id
                logger.debug("Node %s deferred reply to %s (incoming_time: %d)", self.node_id, sender, incoming_time)
            # Duplicate request (retransmission): stay deferred, reply on release.
        else:
            t = await self.clock.tick()
            reply_msg = Message(
                msg_type=MessageType.RA_REPLY,
                sender_id=self.node_id,
                timestamp=t,
                payload={"receiver_id": sender, "req_id": req_id},
            )
            await self.channel.send_message(sender, reply_msg)
            self._total_replies_sent += 1
            logger.debug("Node %s replied immediately to %s", self.node_id, sender)

    async def handle_reply(self, msg: Message) -> None:
        """Processes an incoming RA_REPLY."""
        sender = msg.sender_id
        await self.clock.update(msg.timestamp)

        if self.state == NodeState.WANTED:
            self.replies_received.add(sender)
            self._reply_event.set()
            logger.debug(
                "Node %s got reply from %s (%d/%d)",
                self.node_id,
                sender,
                len(self.replies_received),
                len(self.peers),
            )

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "cs_entries": self._total_cs_entries,
            "requests_sent": self._total_requests_sent,
            "replies_sent": self._total_replies_sent,
            "retransmits_sent": self._total_retransmits_sent,
        }
