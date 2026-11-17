"""Asynchronous TCP channel manager with strict FIFO ordering per peer and timeout controls."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Coroutine, Dict, Optional, Tuple

from distrib_ledger.network.fault_injector import FaultInjector
from distrib_ledger.network.protocol import Message, decode_frames

logger = logging.getLogger(__name__)


class NetworkChannel:
    """Manages asynchronous point-to-point TCP communication with guaranteed FIFO ordering per channel.

    Messages are processed strictly in arrival order per connection (FIFO processing
    semantics), which Chandy-Lamport marker handling relies upon. Outgoing sends hold a
    per-peer lock so transmissions to the same destination are serialized and FIFO.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peer_addresses: Dict[str, Tuple[str, int]],
        fault_injector: Optional[FaultInjector] = None,
        default_timeout_sec: float = 2.0,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addresses = peer_addresses  # node_id -> (host, port)
        self.fault_injector = fault_injector or FaultInjector()
        self.default_timeout_sec = default_timeout_sec

        self._server: Optional[asyncio.Server] = None
        self._incoming_handlers: list[Callable[[Message], Coroutine[None, None, None]]] = []
        self._running = False
        self._message_counter = 0
        self._received_counter = 0

        # Per-peer send lock ensures strict sequential transmission (FIFO) to each peer
        self._peer_locks: Dict[str, asyncio.Lock] = {
            pid: asyncio.Lock() for pid in peer_addresses.keys() if pid != node_id
        }

        # Open inbound connections, used by the control plane to reply on the same socket
        self._conn_writers: Dict[str, asyncio.StreamWriter] = {}
        self._conn_seq = 0

    def register_handler(self, handler: Callable[[Message], Coroutine[None, None, None]]) -> Optional[Callable[[], None]]:
        """Registers a message handler. Returns an unsubscribe callable."""
        self._incoming_handlers.append(handler)

        def _unsubscribe() -> None:
            if handler in self._incoming_handlers:
                self._incoming_handlers.remove(handler)

        return _unsubscribe

    async def start(self) -> None:
        """Starts the TCP listener server."""
        self._running = True
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        logger.info("Node %s listening on %s:%d", self.node_id, self.host, self.port)

    async def stop(self) -> None:
        """Shuts down the server and any open inbound connections."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for writer in list(self._conn_writers.values()):
            writer.close()
        self._conn_writers.clear()
        logger.info("Node %s stopped", self.node_id)

    async def _safe_handler(self, handler: Callable[[Message], Coroutine[None, None, None]], msg: Message) -> None:
        try:
            await handler(msg)
        except Exception as e:
            logger.error("Node %s error in message handler: %s", self.node_id, e)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        buffer = bytearray()
        conn_id = ""
        try:
            peername = writer.get_extra_info("peername")
            self._conn_seq += 1
            conn_id = f"{peername}#{self._conn_seq}"
            self._conn_writers[conn_id] = writer
            while self._running:
                data = await reader.read(4096)
                if not data:
                    break
                buffer.extend(data)
                messages = decode_frames(buffer)
                for msg in messages:
                    # Strict FIFO processing: handle each message to completion before
                    # decoding the next one on this connection.
                    self._received_counter += 1
                    msg.reply_conn = conn_id
                    for handler in self._incoming_handlers:
                        await self._safe_handler(handler, msg)
        except asyncio.CancelledError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            logger.debug("Connection dropped on node %s", self.node_id)
        except Exception as e:
            logger.debug("Socket read ended on node %s: %s", self.node_id, e)
        finally:
            self._conn_writers.pop(conn_id, None)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def send_reply(self, conn_id: str, msg: Message) -> bool:
        """Writes a message back on an open inbound connection (control plane replies).

        Does not count towards the inter-node message counter.
        """
        writer = self._conn_writers.get(conn_id)
        if writer is None or writer.is_closing():
            return False
        try:
            writer.write(msg.encode())
            await writer.drain()
            return True
        except Exception as e:
            logger.debug("Node %s failed to reply on %s: %s", self.node_id, conn_id, e)
            return False

    async def send_message(
        self, target_id: str, msg: Message, timeout_sec: Optional[float] = None
    ) -> bool:
        """Sends a message to target peer with guaranteed FIFO order per peer."""
        if not self._running:
            return False

        if target_id == self.node_id:
            # Deliver locally
            for handler in self._incoming_handlers:
                await self._safe_handler(handler, msg)
            return True

        if target_id not in self.peer_addresses:
            logger.error("Unknown peer %s", target_id)
            return False

        # Intercept via fault injector
        should_send = await self.fault_injector.intercept_outgoing(self.node_id, target_id, msg)
        if not should_send:
            return False

        host, port = self.peer_addresses[target_id]
        timeout = timeout_sec or self.default_timeout_sec
        encoded = msg.encode()

        lock = self._peer_locks.setdefault(target_id, asyncio.Lock())
        async with lock:
            try:
                async with asyncio.timeout(timeout):
                    reader, writer = await asyncio.open_connection(host, port)
                    writer.write(encoded)
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                    self._message_counter += 1
                    return True
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError):
                logger.debug("Connection refused/broken by %s (%s:%d)", target_id, host, port)
                return False
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning("Timeout sending %s from %s to %s (timeout: %.2fs)", msg.msg_type, self.node_id, target_id, timeout)
                return False
            except Exception as e:
                logger.debug("Send failure from %s to %s: %s", self.node_id, target_id, e)
                return False

    async def broadcast(self, msg: Message, exclude_self: bool = True) -> int:
        """Broadcasts a message to all peers concurrently."""
        peers = [pid for pid in self.peer_addresses.keys() if not (exclude_self and pid == self.node_id)]
        tasks = [self.send_message(pid, msg) for pid in peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    @property
    def sent_message_count(self) -> int:
        return self._message_counter

    @property
    def received_message_count(self) -> int:
        return self._received_counter
