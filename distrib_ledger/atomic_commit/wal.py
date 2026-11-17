"""Write-Ahead Log (WAL) with strict disk persistence (fsync)."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class WalRecordType(str, Enum):
    START_2PC = "START_2PC"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    APPLIED = "APPLIED"
    ABORTED = "ABORTED"
    DIRECT_TRANSFER = "DIRECT_TRANSFER"
    CHECKPOINT = "CHECKPOINT"


@dataclass
class WalRecord:
    record_type: WalRecordType
    tx_id: str
    timestamp: float
    data: Dict[str, Any]

    def to_line(self) -> str:
        payload = {
            "type": self.record_type.value,
            "tx_id": self.tx_id,
            "timestamp": self.timestamp,
            "data": self.data,
        }
        return json.dumps(payload) + "\n"

    @classmethod
    def from_line(cls, line: str) -> Optional[WalRecord]:
        line = line.strip()
        if not line:
            return None
        payload = json.loads(line)
        return cls(
            record_type=WalRecordType(payload["type"]),
            tx_id=payload["tx_id"],
            timestamp=payload["timestamp"],
            data=payload.get("data", {}),
        )


class WriteAheadLog:
    """Provides crash-resilient write-ahead logging backed by fsync.

    Recovery tolerates a torn (partially written) trailing record: the log is
    truncated at the first unparsable line, which by construction can only be
    the final record of a process that crashed mid-append.
    """

    def __init__(self, log_path: str | Path):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.log_path, "a+", encoding="utf-8")

    def append(self, record_type: WalRecordType, tx_id: str, data: Optional[Dict[str, Any]] = None, timestamp: Optional[float] = None) -> None:
        """Appends a record and synchronously forces write to persistent storage."""
        t = timestamp if timestamp is not None else time.time()
        record = WalRecord(record_type=record_type, tx_id=tx_id, timestamp=t, data=data or {})
        line = record.to_line()
        self._file.write(line)
        self._file.flush()
        os.fsync(self._file.fileno())

    def replay_records(self) -> list[WalRecord]:
        """Returns every valid record in append order, truncating a torn tail.

        A torn trailing record (crash mid-append) is cut off instead of crashing
        recovery. The returned sequence preserves the exact append order, which
        the balance rebuild relies upon.
        """
        self._file.seek(0)
        records: list[WalRecord] = []
        offset = 0
        truncated = False
        for raw_line in self._file:
            line_offset = offset
            offset += len(raw_line.encode("utf-8"))
            if not raw_line.strip():
                continue
            try:
                rec = WalRecord.from_line(raw_line)
            except (json.JSONDecodeError, KeyError, ValueError):
                # Torn write: only the tail of the log can be partial. Truncate
                # here and ignore everything after it.
                logger.warning(
                    "WAL %s: torn record at byte %d; truncating log tail", self.log_path, line_offset
                )
                self._file.seek(0)
                self._file.truncate(line_offset)
                self._file.flush()
                os.fsync(self._file.fileno())
                truncated = True
                break
            if rec is not None:
                records.append(rec)

        # Reposition file pointer at end for subsequent appends
        self._file.seek(0, os.SEEK_END)
        if truncated:
            logger.info("WAL %s: recovery finished after truncating torn tail", self.log_path)
        return records

    def recover(self) -> Dict[str, Dict[str, Any]]:
        """Reconstructs the transactional state from the on-disk log.

        Returns tx_id -> {status, records, data} with the status taken from the
        last valid record of each transaction.
        """
        transactions: Dict[str, Dict[str, Any]] = {}
        for rec in self.replay_records():
            tx_id = rec.tx_id
            if tx_id not in transactions:
                transactions[tx_id] = {
                    "status": rec.record_type.value,
                    "records": [],
                    "data": rec.data,
                }
            transactions[tx_id]["status"] = rec.record_type.value
            transactions[tx_id]["records"].append(rec)
            if rec.data:
                transactions[tx_id]["data"].update(rec.data)
        return transactions

    def close(self) -> None:
        if not self._file.closed:
            self._file.flush()
            try:
                os.fsync(self._file.fileno())
            except OSError:
                pass
            self._file.close()
