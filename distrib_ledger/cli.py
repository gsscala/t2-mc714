"""Command-Line Interface for starting nodes and operating the cluster."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import uuid
from typing import Any, Dict, Optional, Tuple

from distrib_ledger.network.protocol import Message, MessageType, decode_frames

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("distrib_ledger.cli")


def parse_peers(peers_str: str) -> Dict[str, Tuple[str, int]]:
    """Parses peer string like 'node1=127.0.0.1:8001,node2=127.0.0.1:8002'."""
    peers: Dict[str, Tuple[str, int]] = {}
    if not peers_str.strip():
        return peers
    entries = peers_str.split(",")
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        nid, addr = entry.split("=")
        host, port_str = addr.split(":")
        peers[nid.strip()] = (host.strip(), int(port_str.strip()))
    return peers


def parse_target(target: str) -> Tuple[str, int]:
    host, port_str = target.split(":")
    return host.strip(), int(port_str)


async def send_control(host: str, port: int, op: str, args: Optional[Dict[str, Any]] = None, timeout: float = 30.0) -> Dict[str, Any]:
    """Sends one control command to a node and awaits its CTRL_RESULT."""
    request_id = str(uuid.uuid4())
    msg = Message(
        msg_type=MessageType.CTRL_COMMAND,
        sender_id="driver",
        timestamp=0,
        payload={"op": op, "args": args or {}, "request_id": request_id},
    )
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(msg.encode())
        await writer.drain()
        buffer = bytearray()
        while True:
            remaining = timeout
            data = await asyncio.wait_for(reader.read(4096), timeout=remaining)
            if not data:
                raise ConnectionError("Node closed the connection before replying")
            buffer.extend(data)
            for reply in decode_frames(buffer):
                if reply.msg_type == MessageType.CTRL_RESULT and reply.payload.get("request_id") == request_id:
                    return reply.payload
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def run_node(args: argparse.Namespace) -> None:
    from distrib_ledger.node import DistributedNode

    peer_addresses = parse_peers(args.peers)
    node = DistributedNode(
        node_id=args.id,
        host=args.host,
        port=args.port,
        peer_addresses=peer_addresses,
        wal_dir=args.wal_dir,
        initial_balance=args.balance,
        auditor_id=args.auditor,
    )
    await node.start()
    logger.info("Node %s running (recovery: %s). Press Ctrl+C to stop.", args.id, node.last_recovery or "clean")

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await node.stop()


async def run_client(args: argparse.Namespace) -> None:
    host, port = parse_target(args.target)
    op, ctrl_args, timeout = _build_op(args)

    try:
        payload = await send_control(host, port, op, ctrl_args, timeout=timeout)
    except (ConnectionError, TimeoutError, OSError) as e:
        # Expected when the target crashed or is unreachable: report cleanly.
        print(json.dumps({"ok": False, "op": op, "error": str(e)}))
        sys.exit(2)

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if not payload.get("ok", False):
        sys.exit(1)


def _build_op(args: argparse.Namespace) -> Tuple[str, Dict[str, Any], float]:
    """Maps client subcommand arguments to a control-plane operation."""
    cmd = args.client_cmd
    if cmd == "ping":
        return "ping", {}, 5.0
    if cmd == "stats":
        return "get_stats", {}, 10.0
    if cmd == "transfer":
        return "transfer", {
            "to": args.to,
            "amount": args.amount,
            "use_cs": not args.no_cs,
        }, 60.0
    if cmd == "snapshot":
        return "snapshot", {"snapshot_id": args.id, "timeout_sec": args.timeout}, args.timeout + 10.0
    if cmd == "audit":
        return "audit", {"snapshot_id": args.id, "timeout_sec": args.timeout}, args.timeout + 10.0
    if cmd == "cs":
        return "request_cs", {}, 60.0
    if cmd == "cs-release":
        return "release_cs", {}, 10.0
    if cmd == "load":
        return "load", {
            "target": args.to,
            "count": args.count,
            "rate": args.rate,
            "amount": args.amount,
        }, 10.0
    if cmd == "fault":
        fault_args: Dict[str, Any] = {}
        if args.seed is not None:
            fault_args["seed"] = args.seed
        if args.drop is not None:
            fault_args["drop_rate"] = args.drop
        if args.delay_ms is not None:
            fault_args["delay_mean_ms"] = args.delay_ms
            fault_args["delay_jitter_ms"] = args.jitter_ms
        if args.heal:
            fault_args["heal_partitions"] = True
        if args.partition:
            fault_args["partition_peers"] = [p.strip() for p in args.partition.split(",") if p.strip()]
        return "set_fault", fault_args, 10.0
    if cmd == "crash":
        # The node dies without replying; tolerate the broken connection.
        return "crash", {}, 2.0
    raise ValueError(f"Unknown client command: {cmd}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed Ledger CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Node command
    node_parser = subparsers.add_parser("node", help="Run a distributed node")
    node_parser.add_argument("--id", required=True, help="Node ID (e.g. node1)")
    node_parser.add_argument("--host", default="0.0.0.0", help="Host address to bind")
    node_parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    node_parser.add_argument("--peers", default="", help="Comma-separated list of peers (id=host:port)")
    node_parser.add_argument("--wal-dir", default="wal_data", help="Directory for WAL storage")
    node_parser.add_argument("--balance", type=float, default=1000.0, help="Initial account balance")
    node_parser.add_argument("--auditor", default=None, help="Auditor node ID")
    node_parser.set_defaults(func=run_node)

    # Client commands
    client_parser = subparsers.add_parser("client", help="Send control commands to a running node")
    client_parser.add_argument("--target", required=True, help="Node address as host:port")
    client_parser.set_defaults(func=run_client)
    client_sub = client_parser.add_subparsers(dest="client_cmd", required=True)

    client_sub.add_parser("ping", help="Check node responsiveness")
    client_sub.add_parser("stats", help="Query node statistics and state")

    p = client_sub.add_parser("transfer", help="Atomic cross-node transfer via 2PC")
    p.add_argument("--to", required=True, help="Destination node ID")
    p.add_argument("--amount", type=float, required=True)
    p.add_argument("--no-cs", action="store_true", help="Skip Ricart-Agrawala coordination lock")

    p = client_sub.add_parser("snapshot", help="Trigger a Chandy-Lamport snapshot from this node")
    p.add_argument("--id", required=True, help="Snapshot ID")
    p.add_argument("--timeout", type=float, default=30.0)

    p = client_sub.add_parser("audit", help="Verify global conservation for a completed snapshot (auditor node)")
    p.add_argument("--id", required=True, help="Snapshot ID")
    p.add_argument("--timeout", type=float, default=5.0)

    client_sub.add_parser("cs", help="Request and hold the Ricart-Agrawala critical section")
    client_sub.add_parser("cs-release", help="Release the critical section")

    p = client_sub.add_parser("load", help="Generate background direct-transfer traffic")
    p.add_argument("--to", required=True, help="Destination node ID")
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--rate", type=float, default=25.0, help="Transfers per second")
    p.add_argument("--amount", type=float, default=10.0)

    p = client_sub.add_parser("fault", help="Configure this node's fault injector")
    p.add_argument("--drop", type=float, default=None, help="Packet drop rate in [0,1]")
    p.add_argument("--delay-ms", type=float, default=None, help="Mean injected delay (ms)")
    p.add_argument("--jitter-ms", type=float, default=0.0, help="Delay jitter (ms)")
    p.add_argument("--partition", default=None, help="Comma-separated peer IDs to partition from this node")
    p.add_argument("--heal", action="store_true", help="Heal all partitions")
    p.add_argument("--seed", type=int, default=None, help="PRNG seed for deterministic faults")

    client_sub.add_parser("crash", help="Abruptly kill the target node (tests recovery)")

    args = parser.parse_args()
    if args.command == "node":
        try:
            asyncio.run(args.func(args))
        except KeyboardInterrupt:
            logger.info("Exiting cleanly.")
    elif args.command == "client":
        asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
