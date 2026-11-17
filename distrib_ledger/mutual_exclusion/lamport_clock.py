"""Lamport Logical Clock implementation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class LamportClock:
    """Scalar Lamport logical clock with thread and coroutine safety."""

    node_id: str
    _value: int = 0

    def __post_init__(self):
        self._lock = asyncio.Lock()

    @property
    def value(self) -> int:
        return self._value

    async def tick(self) -> int:
        """Increments clock on internal event."""
        async with self._lock:
            self._value += 1
            return self._value

    async def update(self, received_time: int) -> int:
        """Updates clock upon receiving a message with timestamp received_time."""
        async with self._lock:
            self._value = max(self._value, int(received_time)) + 1
            return self._value

    def is_less(self, t1: int, id1: str, t2: int, id2: str) -> bool:
        """Strict total order relation for events."""
        if t1 < t2:
            return True
        if t1 == t2 and id1 < id2:
            return True
        return False
