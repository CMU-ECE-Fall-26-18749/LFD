"""
Local Fault Detector (LFD1) — 18-749 Milestone 1

Usage:
    python lfd.py --lfd_id LFD1 --replica_id S1 \
                  --server_url ws://localhost:8001 \
                  --heartbeat_freq 5

"""

import argparse
import asyncio
import json

import websockets

from utils import log

parser = argparse.ArgumentParser(description="Local Fault Detector")
parser.add_argument("--lfd_id", type=str, default="LFD1")
parser.add_argument("--replica_id", type=str, default="S1")
parser.add_argument(
    "--server_url",
    type=str,
    default="ws://localhost:8001",
    help="WebSocket base URL of the server replica to heartbeat",
)
parser.add_argument(
    "--heartbeat_freq",
    type=float,
    required=True,
    help="Heartbeat interval in seconds (REQUIRED — must not be hard-coded)",
)
parser.add_argument(
    "--timeout",
    type=float,
    default=3.0,
    help="Seconds to wait for a heartbeat reply before declaring failure (default: 3)",
)
args = parser.parse_args()

LFD_ID = args.lfd_id
REPLICA_ID = args.replica_id
SERVER_URL = args.server_url
HEARTBEAT_FREQ = args.heartbeat_freq
TIMEOUT = args.timeout


async def run_lfd():
    """
    Continuously heartbeat the server replica.
    If the connection drops or a reply times out, declare the replica dead.
    """
    heartbeat_count = 1
    url = f"{SERVER_URL}/ws/heartbeat"

    while True:
        try:
            async with websockets.connect(url) as ws:
                log(
                    f"{LFD_ID}: Connected to {REPLICA_ID} for heartbeating "
                    f"(freq={HEARTBEAT_FREQ}s)",
                    "heartbeat",
                )

                while True:
                    # ── Send heartbeat ──
                    ping_msg = {
                        "lfd_id": LFD_ID,
                        "replica_id": REPLICA_ID,
                        "heartbeat_count": heartbeat_count,
                    }
                    await ws.send(json.dumps(ping_msg))
                    log(
                        f"{LFD_ID}: [{heartbeat_count}] Sending heartbeat to {REPLICA_ID}",
                        "heartbeat",
                    )

                    # ── Wait for reply with timeout ──
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT)
                        reply = json.loads(raw)
                        log(
                            f"{LFD_ID}: [{heartbeat_count}] Received heartbeat from "
                            f"{reply.get('replica_id', REPLICA_ID)}",
                            "heartbeat",
                        )
                    except asyncio.TimeoutError:
                        log(
                            f"{LFD_ID}: [{heartbeat_count}] HEARTBEAT TIMEOUT — "
                            f"{REPLICA_ID} is not responding!",
                            "fault",
                        )
                        # In later milestones, this would notify the GFD.
                        # For now, just report and keep trying.

                    heartbeat_count += 1
                    await asyncio.sleep(HEARTBEAT_FREQ)

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            log(
                f"{LFD_ID}: [{heartbeat_count}] FAULT DETECTED — "
                f"Lost connection to {REPLICA_ID} ({type(e).__name__}). "
                f"Replica {REPLICA_ID} has crashed!",
                "fault",
            )
            log(
                f"{LFD_ID}: Waiting for {REPLICA_ID} to come back (retrying in "
                f"{HEARTBEAT_FREQ}s)...",
                "fault",
            )
            heartbeat_count += 1
            await asyncio.sleep(HEARTBEAT_FREQ)


if __name__ == "__main__":
    log(
        f"{LFD_ID}: Starting — monitoring {REPLICA_ID} at {SERVER_URL}, "
        f"heartbeat_freq={HEARTBEAT_FREQ}s",
        "info",
    )
    asyncio.run(run_lfd())