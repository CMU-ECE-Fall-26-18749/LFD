"""Local fault detector: monitor one replica and report membership to the GFD."""

import argparse
import asyncio
import contextlib
import json

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from utils import log


class LocalFaultDetector:
    def __init__(self, args):
        self.args = args
        self.alive = False
        self.boot_id = None
        self.changed = asyncio.Event()

    def set_status(self, alive, boot_id=None):
        if (alive, boot_id) != (self.alive, self.boot_id):
            self.alive, self.boot_id = alive, boot_id
            self.changed.set()
            log(f"{self.args.lfd_id}: {'add' if alive else 'delete'} replica "
                f"{self.args.replica_id}", "membership")

    async def monitor(self):
        args = self.args
        count = 0
        while True:
            try:
                async with connect(args.server_url.rstrip("/") + "/ws/heartbeat",
                                   open_timeout=args.timeout, close_timeout=1,
                                   proxy=None) as ws:
                    while True:
                        count += 1
                        log(f"{args.lfd_id}: [{count}] Sending heartbeat to {args.replica_id}", "heartbeat")
                        await asyncio.wait_for(ws.send(json.dumps({
                            "lfd_id": args.lfd_id, "replica_id": args.replica_id,
                            "heartbeat_count": count})), args.timeout)
                        reply = json.loads(await asyncio.wait_for(ws.recv(), args.timeout))
                        if (reply.get("replica_id") != args.replica_id or
                                reply.get("heartbeat_count") != count or reply.get("status") != "alive"):
                            raise ValueError("heartbeat identity/count mismatch")
                        log(f"{args.lfd_id}: [{count}] Received heartbeat from {args.replica_id}", "heartbeat")
                        self.set_status(True, reply.get("boot_id"))
                        await asyncio.sleep(args.heartbeat_freq)
            except (OSError, TimeoutError, WebSocketException, ValueError) as exc:
                log(f"{args.lfd_id}: [{count}] FAULT DETECTED - {args.replica_id}: "
                    f"{type(exc).__name__}", "fault")
                self.set_status(False)
                await asyncio.sleep(args.heartbeat_freq)

    async def control(self):
        args = self.args
        while True:
            tasks = []
            try:
                async with connect(args.gfd_url.rstrip("/") + f"/ws/lfd/{args.lfd_id}",
                                   open_timeout=args.timeout, close_timeout=1,
                                   proxy=None) as ws:
                    lock = asyncio.Lock()

                    async def send(msg):
                        async with lock:
                            await asyncio.wait_for(ws.send(json.dumps(msg)), args.timeout)

                    await send({"type": "register", "replica_id": args.replica_id,
                                "url": args.advertise_url or args.server_url})
                    log(f"{args.lfd_id}: Registered with GFD", "info")
                    self.changed.set()

                    async def report():
                        while True:
                            await self.changed.wait()
                            self.changed.clear()
                            await send({"type": "status", "alive": self.alive,
                                        "boot_id": self.boot_id})
                            log(f"{args.lfd_id}: Sent {'add' if self.alive else 'delete'} "
                                f"replica {args.replica_id} to GFD", "membership")

                    async def receive():
                        async for raw in ws:
                            msg = json.loads(raw)
                            if msg.get("type") == "heartbeat":
                                count = msg["heartbeat_count"]
                                log(f"{args.lfd_id}: [{count}] Received heartbeat from GFD", "heartbeat")
                                await send({"type": "heartbeat_reply", "heartbeat_count": count})
                                log(f"{args.lfd_id}: [{count}] Sent heartbeat reply to GFD", "heartbeat")

                    tasks = [asyncio.create_task(report()), asyncio.create_task(receive())]
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
            except (OSError, TimeoutError, WebSocketException, ValueError) as exc:
                log(f"{args.lfd_id}: GFD connection lost ({type(exc).__name__}); retrying", "fault")
            finally:
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            await asyncio.sleep(min(args.heartbeat_freq, 1))

    async def run(self):
        if self.args.gfd_url:
            await asyncio.gather(self.monitor(), self.control())
        else:
            await self.monitor()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lfd_id", default="LFD1")
    parser.add_argument("--replica_id", default="S1")
    parser.add_argument("--server_url", default="ws://127.0.0.1:8001")
    parser.add_argument("--heartbeat_freq", type=float, required=True,
                        help="Heartbeat INTERVAL in seconds (retained Milestone 1 flag name)")
    parser.add_argument("--timeout", type=float, default=3)
    parser.add_argument("--gfd_url", help="Enable Milestone 2 reporting, e.g. ws://192.168.1.10:9000")
    parser.add_argument("--advertise_url", help="Replica URL reachable from the CLIENT machine")
    args = parser.parse_args()
    if args.heartbeat_freq <= 0 or args.timeout <= 0:
        parser.error("heartbeat_freq and timeout must be positive seconds")
    return args


if __name__ == "__main__":
    try:
        asyncio.run(LocalFaultDetector(parse_args()).run())
    except KeyboardInterrupt:
        pass
