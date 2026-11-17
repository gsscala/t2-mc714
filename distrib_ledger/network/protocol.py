"""Network message definitions and framing protocol for distrib_ledger."""

from __future__ import annotations

import json
import struct
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class MessageType(str, Enum):
    # Ricart-Agrawala Mutual Exclusion
    RA_REQUEST = "RA_REQUEST"
    RA_REPLY = "RA_REPLY"

    # Two-Phase Commit
    TPC_PREPARE = "TPC_PREPARE"
    TPC_VOTE_COMMIT = "TPC_VOTE_COMMIT"
    TPC_VOTE_ABORT = "TPC_VOTE_ABORT"
    TPC_GLOBAL_COMMIT = "TPC_GLOBAL_COMMIT"
    TPC_GLOBAL_ABORT = "TPC_GLOBAL_ABORT"
    TPC_ACK = "TPC_ACK"
    TPC_QUERY_STATUS = "TPC_QUERY_STATUS"
    TPC_STATUS_REPLY = "TPC_STATUS_REPLY"

    # Chandy-Lamport Snapshot
    CL_MARKER = "CL_MARKER"
    CL_TRANSFER = "CL_TRANSFER"
    CL_SNAPSHOT_REPORT = "CL_SNAPSHOT_REPORT"

    # Control plane: driver/client commands and node replies
    CTRL_COMMAND = "CTRL_COMMAND"
    CTRL_RESULT = "CTRL_RESULT"


@dataclass
class Message:
    msg_type: MessageType
    sender_id: str
    timestamp: int
    payload: Dict[str, Any] = field(default_factory=dict)
    msg_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_type": self.msg_type.value if isinstance(self.msg_type, MessageType) else str(self.msg_type),
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
            "msg_id": self.msg_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Message:
        return cls(
            msg_type=MessageType(data["msg_type"]),
            sender_id=data["sender_id"],
            timestamp=int(data["timestamp"]),
            payload=data.get("payload", {}),
            msg_id=data.get("msg_id"),
        )

    def encode(self) -> bytes:
        raw_json = json.dumps(self.to_dict()).encode("utf-8")
        # 4-byte big-endian length prefix followed by JSON payload
        prefix = struct.pack("!I", len(raw_json))
        return prefix + raw_json


def decode_frames(buffer: bytearray) -> list[Message]:
    """Extract complete messages from a streaming byte buffer."""
    messages: list[Message] = []
    while len(buffer) >= 4:
        length = struct.unpack("!I", buffer[:4])[0]
        if len(buffer) < 4 + length:
            break
        body = buffer[4 : 4 + length]
        del buffer[: 4 + length]
        try:
            data = json.loads(body.decode("utf-8"))
            messages.append(Message.from_dict(data))
        except Exception:
            continue
    return messages
