import asyncio
import json
import logging
import os
import random
import string
import threading
import time
from dataclasses import dataclass
from typing import Set, Dict, Any, Optional, List, Tuple

import websockets
from aiohttp import web  # HTTP health check for Render

from loootcoin import LoootCoin, Transaction, Block
from wallet import load_or_create_wallet, generate_wallet, sign_message

# ---------------- Configuration / Constants ---------------- #
PROTOCOL_VERSION = 1
SYSTEM_SENDER = "SYSTEM"
PEERS_FILE = "peers.json"
HEARTBEAT_INTERVAL = 20  # seconds
HELLO_INTERVAL = 30      # seconds
BROADCAST_TIMEOUT = 8    # seconds per peer
RETRY_BACKOFF_BASE = 2   # exponential backoff base
RETRY_BACKOFF_MAX = 300  # max seconds between retries per peer


@dataclass
class PeerState:
    url: str
    failures: int = 0
    next_retry_at: float = 0.0

    def backoff(self) -> None:
        self.failures += 1
        delay = min(RETRY_BACKOFF_MAX, (RETRY_BACKOFF_BASE ** min(self.failures, 8)))
        self.next_retry_at = time.time() + delay

    def ok(self) -> None:
        self.failures = 0
        self.next_retry_at = 0.0


# ---------------- Node class ---------------- #
class Node:
    def __init__(self, port: int, peers: Set[str], wallet_file: str, data_dir: str = "data"):
        os.makedirs(data_dir, exist_ok=True)
        self.port = port
        self.data_dir = data_dir
        self.peers_path = os.path.join(data_dir, PEERS_FILE)

        # Peer state map
        initial_peers = set(peers or []) | self._load_peers_from_disk()
        self.peer_states: Dict[str, PeerState] = {p: PeerState(p) for p in initial_peers if p != self._self_url}

        self.blockchain = LoootCoin(data_dir=data_dir)

        w = load_or_create_wallet(wallet_file)
        self.address = w["address"]

        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.server: Optional[websockets.server.Serve] = None
        self.http_runner: Optional[web.AppRunner] = None
        self.mining = False
        self._mine_future: Optional[asyncio.Future] = None

        self._shutdown_event: Optional[asyncio.Event] = None
        self._tasks: List[asyncio.Task] = []

        # Logging setup
        self.logger = logging.getLogger(f"Node:{port}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            handler.setFormatter(fmt)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    # ---------------- Properties ---------------- #
    @property
    def _self_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    @property
    def peer_urls(self) -> Set[str]:
        return set(self.peer_states.keys())

    # ---------------- Persistence ---------------- #
    def _load_peers_from_disk(self) -> Set[str]:
        try:
            path = os.path.join(self.data_dir, PEERS_FILE)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {u for u in data if isinstance(u, str)}
        except Exception as e:
            self.logger.warning(f"Failed to load peers file: {e}")
        return set()

    def _save_peers_to_disk(self) -> None:
        try:
            with open(self.peers_path, "w", encoding="utf-8") as f:
                json.dump(sorted(self.peer_urls), f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to save peers: {e}")

    # ---------------- HTTP health check ---------------- #
    async def _http_handler(self, request):
        return web.Response(text="🌐 LoootCoin node is running. Use a WebSocket client.")

    async def _start_http_server(self):
        app = web.Application()
        app.router.add_get("/", self._http_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.getenv("PORT", self.port))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

        self.logger.info(f"🌍 HTTP health endpoint listening on http://0.0.0.0:{port}")
        return runner

    # ---------------- Networking ---------------- #
    async def _handler(self, websocket):
        self.logger.info(f"Incoming connection from {websocket.remote_address}")
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            # Handle HELLO / PEERS / TX / BLOCK / REPLACE_CHAIN ...
            # (same logic as before in your old Node)

    # ---------------- Mining ---------------- #
    async def _mine_loop(self):
        self.logger.info("Mining loop started")
        while self.mining and not self._shutdown_event.is_set():
            block = self.blockchain.mine_block(self.address)
            if block:
                await self._broadcast({"type": "BLOCK", "block": self._block_to_network(block)})
            await asyncio.sleep(1.5)
        self.logger.info("Mining loop stopped")

    def start_mining(self):
        if not self.loop or self.mining:
            return
        self.mining = True
        self._mine_future = asyncio.run_coroutine_threadsafe(self._mine_loop(), self.loop)

    def stop_mining(self):
        self.mining = False

    # ---------------- Server control ---------------- #
    async def _main_async(self):
        self._shutdown_event = asyncio.Event()

        # Start WebSocket server
        self.server = await websockets.serve(self._handler, "0.0.0.0", self.port)
        self.logger.info(f"🌐 WebSocket node listening on {self._self_url}")

        # Start HTTP health endpoint
        self.http_runner = await self._start_http_server()

        await self._shutdown_event.wait()

        # Graceful shutdown
        self.server.close()
        await self.server.wait_closed()
        if self.http_runner:
            await self.http_runner.cleanup()

    def start(self):
        def runner():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.create_task(self._main_async())
            try:
                self.loop.run_forever()
            finally:
                pending = asyncio.all_tasks(self.loop)
                for task in pending:
                    task.cancel()
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                self.loop.close()
        threading.Thread(target=runner, daemon=True).start()

    def shutdown(self):
        if self.loop and self._shutdown_event and not self._shutdown_event.is_set():
            self.loop.call_soon_threadsafe(self._shutdown_event.set)
            self.loop.call_soon_threadsafe(self.loop.stop)


# ---------------- CLI Runner ---------------- #
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", 5001)))
    parser.add_argument("--wallet", type=str, default="wallet.json")
    parser.add_argument("--data", type=str, default="data")
    parser.add_argument("--peers", nargs="*", default=[])
    args = parser.parse_args()

    n = Node(args.port, set(args.peers), args.wallet, data_dir=args.data)
    n.start()
    print(f"🌐 Node listening {n._self_url}")
    print(f"💳 Address: {n.address}")

    if os.getenv("RENDER"):
        # Non-interactive mode for Render
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            n.shutdown()
    else:
        # Local interactive CLI
        try:
            while True:
                cmd = input("(tx/mine/stop/peers/bal/quit)> ").strip().lower()
                if cmd == "tx":
                    to = input("recipient: ").strip()
                    amt = float(input("amount: "))
                    print("OK" if n.create_transaction(to, amt) else "FAILED")
                elif cmd == "mine":
                    n.start_mining(); print("mining on")
                elif cmd == "stop":
                    n.stop_mining(); print("mining off")
                elif cmd == "peers":
                    print(sorted(n.peer_urls))
                elif cmd == "bal":
                    from pprint import pprint; pprint(n.blockchain.balances)
                elif cmd == "quit":
                    n.shutdown(); break
        except KeyboardInterrupt:
            n.shutdown()


# ---------------- BotManager ---------------- #
class BotManager:
    def __init__(self, node: Node, num_bots: int = 3):
        self.node = node
        self.bots = []
        for _ in range(num_bots):
            priv, pub, addr = generate_wallet()
            name = "Bot_" + "".join(random.choices(string.ascii_uppercase, k=4))
            self.bots.append({"name": name, "private": priv, "public": pub, "address": addr})

        self.running = False
        self.thread: Optional[threading.Thread] = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            if not self.bots:
                time.sleep(5)
                continue
            sender = random.choice(self.bots)
            receiver = random.choice(self.bots)
            if sender["address"] == receiver["address"]:
                continue
            amount = round(random.uniform(0.1, 2.0), 2)
            message = f"{sender['address']}->{receiver['address']}:{amount}"
            signature = sign_message(sender["private"], message)

            tx = Transaction(sender["address"], receiver["address"], amount)
            if hasattr(tx, "signature"):
                tx.signature = signature
            if hasattr(tx, "public_key"):
                tx.public_key = sender["public"]

            ok = self.node.blockchain.add_transaction(tx)
            if ok and self.node.loop:
                asyncio.run_coroutine_threadsafe(
                    self.node._broadcast({"type": "TX", "tx": tx.to_dict()}), self.node.loop
                )
            print(f"[BOT] {sender['name']} sent {amount} LC to {receiver['name']}")
            time.sleep(random.randint(10, 20))
