"""Deterministic fault injection layer for network and node failures."""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import Optional, Set, Tuple

from distrib_ledger.network.protocol import Message

logger = logging.getLogger(__name__)


@dataclass
class FaultConfig:
    seed: Optional[int] = None
    packet_drop_rate: float = 0.0  # Probability in [0.0, 1.0]
    delay_mean_ms: float = 0.0     # Mean latency in ms
    delay_jitter_ms: float = 0.0   # Jitter spread in ms
    partitioned_pairs: Set[Tuple[str, str]] = field(default_factory=set)
    crashed_nodes: Set[str] = field(default_factory=set)


class FaultInjector:
    """Simulates realistic network impairments and node failures deterministically."""

    def __init__(self, config: Optional[FaultConfig] = None):
        self.config = config or FaultConfig()
        self.rng = random.Random(self.config.seed) if self.config.seed is not None else random.Random()
        self._dropped_count = 0
        self._delayed_count = 0
        self._total_evaluated = 0

    def set_seed(self, seed: int) -> None:
        self.config.seed = seed
        self.rng = random.Random(seed)

    def set_drop_rate(self, rate: float) -> None:
        self.config.packet_drop_rate = max(0.0, min(1.0, rate))

    def set_delay(self, mean_ms: float, jitter_ms: float = 0.0) -> None:
        self.config.delay_mean_ms = max(0.0, mean_ms)
        self.config.delay_jitter_ms = max(0.0, jitter_ms)

    def partition(self, node_a: str, node_b: str) -> None:
        pair = tuple(sorted((node_a, node_b)))
        self.config.partitioned_pairs.add(pair)
        logger.warning("Partition added between %s and %s", node_a, node_b)

    def heal_partition(self, node_a: str, node_b: str) -> None:
        pair = tuple(sorted((node_a, node_b)))
        self.config.partitioned_pairs.discard(pair)
        logger.info("Partition healed between %s and %s", node_a, node_b)

    def heal_all_partitions(self) -> None:
        self.config.partitioned_pairs.clear()
        logger.info("All network partitions healed")

    def crash_node(self, node_id: str) -> None:
        self.config.crashed_nodes.add(node_id)
        logger.warning("Node %s marked as crashed", node_id)

    def recover_node(self, node_id: str) -> None:
        self.config.crashed_nodes.discard(node_id)
        logger.info("Node %s recovered from crash", node_id)

    def is_node_crashed(self, node_id: str) -> bool:
        return node_id in self.config.crashed_nodes

    def is_partitioned(self, node_a: str, node_b: str) -> bool:
        pair = tuple(sorted((node_a, node_b)))
        return pair in self.config.partitioned_pairs

    async def intercept_outgoing(self, sender_id: str, target_id: str, msg: Message) -> bool:
        """Evaluates whether to deliver, delay, or drop a message.
        
        Returns:
            True if message should be delivered.
            False if message was dropped.
        """
        self._total_evaluated += 1

        if self.is_node_crashed(sender_id) or self.is_node_crashed(target_id):
            self._dropped_count += 1
            return False

        if self.is_partitioned(sender_id, target_id):
            self._dropped_count += 1
            return False

        if self.config.packet_drop_rate > 0.0:
            if self.rng.random() < self.config.packet_drop_rate:
                self._dropped_count += 1
                return False

        # Apply latency simulation if configured
        if self.config.delay_mean_ms > 0.0:
            jitter = (self.rng.random() * 2 - 1) * self.config.delay_jitter_ms
            delay_sec = max(0.0, (self.config.delay_mean_ms + jitter) / 1000.0)
            if delay_sec > 0:
                self._delayed_count += 1
                await asyncio.sleep(delay_sec)

        return True

    @property
    def stats(self) -> dict:
        return {
            "total_evaluated": self._total_evaluated,
            "dropped_count": self._dropped_count,
            "delayed_count": self._delayed_count,
        }
