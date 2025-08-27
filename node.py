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

from aiohttp import web, ClientSession, WSMsgType  # HTTP + WS server & client

from loootcoin import LoootCoin, Transaction, Block
from wallet import load_or_create_wallet, generate_wallet, sign_message

# ---------------- Configuration / Constants ---------------- #
PROTOCOL_VERSION = 1
SYSTEM_SENDER = "SYSTEM"
PEERS_FILE = "peers.json"
HEARTBEAT_INTERVAL = 20  # seconds
HELLO_INTERVAL = 30      # seconds
CONNECT_TIMEOUT = 8      # seconds per peer connect
RETRY_BACKOFF_BASE = 2
RETRY_BACKOFF_MAX = 300
WS_PATH = os.getenv("WS_PATH", "/ws")  # WebSocket path served by this node


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


class Node:
    def __init__(self, port: int, peers: Set[str], wallet_file: str, data_dir: str = "data"):
        os.makedirs(data_dir, exist_ok=True)
        self.port = port
        self.data_dir = data_dir
        self.peers_path = os.path.join(data_dir, PEERS_FILE)

        # Public URL handling
        self.public_url = os.getenv("RENDER_EXTERNAL_URL")  # Render sets this automatically
        if self.public_url:
            # Always use wss:// scheme + append /ws
            self.advertised_url = self.public_url.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
        else:
            # Local fallback
            self.advertised_url = f"ws://127.0.0.1:{self.port}/ws"

        # Peer state map
        initial_peers = set(peers or []) | self._load_peers_from_disk()
        self.peer_states: Dict[str, PeerState] = {
            p: PeerState(p) for p in initial_peers if p != self.advertised_url
        }

        self.blockchain = LoootCoin(data_dir=data_dir)

        w = load_or_create_wallet(wallet_file)
        self.address = w["address"]

        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.server: Optional[websockets.server.Serve] = None
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
        return self.advertised_url

    @property
    def advertised_url(self) -> str:
        """
        What we share with peers. Set PUBLIC_WS_URL in cloud like:
        wss://your-service.onrender.com/ws
        """
        return os.getenv("PUBLIC_WS_URL", self._self_url)

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

    # ---------------- HTTP + WebSocket server ---------------- #
    async def _http_root(self, request):
        return web.Response(text="🌐 LoootCoin node is running. Use a WebSocket client at path: %s" % WS_PATH)

    async def _ws_handler(self, request: web.Request):
        ws = web.WebSocketResponse(heartbeat=30.0)
        await ws.prepare(request)
        self.logger.info(f"WS connect from {request.remote}")

        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except Exception:
                    continue

                if payload.get("protocol", PROTOCOL_VERSION) != PROTOCOL_VERSION:
                    continue

                t = payload.get("type")
                if t == "HELLO":
                    await ws.send_str(json.dumps({
                        "protocol": PROTOCOL_VERSION,
                        "type": "PEERS",
                        "peers": list(self.peer_urls | {self.advertised_url}),
                    }))
                    await ws.send_str(json.dumps({
                        "protocol": PROTOCOL_VERSION,
                        "type": "TIP",
                        "index": len(self.blockchain.chain) - 1,
                        "hash": self.blockchain.chain[-1].hash,
                    }))

                elif t == "PEERS":
                    for p in payload.get("peers", []):
                        if isinstance(p, str) and p != self._self_url:
                            if p not in self.peer_states:
                                self.peer_states[p] = PeerState(p)
                    self._save_peers_to_disk()

                elif t == "TX":
                    txd = payload.get("tx")
                    if not txd:
                        continue
                    try:
                        tx = self._tx_from_any(txd)
                    except Exception:
                        continue
                    if self._validate_transaction(tx):
                        if self.blockchain.add_transaction(tx):
                            await self._broadcast({"type": "TX", "tx": tx.to_dict()})

                elif t == "BLOCK":
                    bd = payload.get("block")
                    if not bd:
                        continue
                    try:
                        block = self._block_from_network(bd)
                    except Exception:
                        continue
                    if self._validate_block_against_tip(block):
                        self._apply_block_and_clean_pool(block)
                        await self._broadcast({"type": "BLOCK", "block": self._block_to_network(block)})

                elif t == "REPLACE_CHAIN":
                    chain_dicts = payload.get("chain", [])
                    try:
                        new_chain = [self._block_from_network(bd) for bd in chain_dicts]
                    except Exception:
                        continue
                    if len(new_chain) > len(self.blockchain.chain) and self._is_chain_valid(new_chain):
                        self.logger.info("Replacing with longer valid chain")
                        self.blockchain.chain = [new_chain[0]]
                        self.blockchain.balances = {}
                        for b in new_chain[1:]:
                            self._apply_block_and_clean_pool(b, mempool_clean=False)
                        self.blockchain._save_chain()
                        self.blockchain._save_balances()

            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                break
            elif msg.type == WSMsgType.ERROR:
                self.logger.debug(f"WS error: {ws.exception()}")
                break

        return ws

    async def _create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/", self._http_root)
        app.router.add_get(WS_PATH, self._ws_handler)
        return app

    # ---------------- Outbound networking ---------------- #
    async def _broadcast(self, message: Dict[str, Any]):
        message = {**message, "protocol": PROTOCOL_VERSION}
        stale: Set[str] = set()
        async with ClientSession() as session:
            for url in list(self.peer_urls):
                state = self.peer_states.get(url)
                if not state:
                    continue
                now = time.time()
                if state.next_retry_at and now < state.next_retry_at:
                    continue
                try:
                    async with session.ws_connect(url, timeout=CONNECT_TIMEOUT, autoping=True) as ws:
                        await ws.send_str(json.dumps(message))
                        state.ok()
                except Exception:
                    state.backoff()
                    if state.failures > 5:
                        stale.add(url)
        for s in stale:
            self.peer_states.pop(s, None)
        if stale:
            self._save_peers_to_disk()

    async def _say_hello(self):
        async with ClientSession() as session:
            for url in list(self.peer_urls):
                state = self.peer_states.get(url)
                if not state:
                    continue
                now = time.time()
                if state.next_retry_at and now < state.next_retry_at:
                    continue
                try:
                    async with session.ws_connect(url, timeout=CONNECT_TIMEOUT, autoping=True) as ws:
                        await ws.send_str(json.dumps({"protocol": PROTOCOL_VERSION, "type": "HELLO"}))
                        state.ok()
                except Exception:
                    state.backoff()

    async def _heartbeat(self):
        while not self._shutdown_event.is_set():
            remove: Set[str] = set()
            async with ClientSession() as session:
                for url, state in list(self.peer_states.items()):
                    now = time.time()
                    if state.next_retry_at and now < state.next_retry_at:
                        continue
                    try:
                        async with session.ws_connect(url, timeout=5, autoping=True) as ws:
                            # a quick open/close acts like a probe
                            state.ok()
                    except Exception:
                        state.backoff()
                        if state.failures > 8:
                            remove.add(url)
            for r in remove:
                self.logger.info(f"Dropping stale peer {r}")
                self.peer_states.pop(r, None)
                self._save_peers_to_disk()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _hello_task(self):
        while not self._shutdown_event.is_set():
            await self._say_hello()
            await asyncio.sleep(HELLO_INTERVAL)

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

    # ---------------- Client API ---------------- #
    def create_transaction(self, recipient: str, amount: float) -> bool:
        tx = Transaction(self.address, recipient, float(amount))
        if not self._validate_transaction(tx):
            return False
        ok = self.blockchain.add_transaction(tx)
        if ok and self.loop:
            asyncio.run_coroutine_threadsafe(
                self._broadcast({"type": "TX", "tx": tx.to_dict()}), self.loop
            )
        return ok

    # ---------------- Main server control ---------------- #
    async def _main_async(self):
        self._shutdown_event = asyncio.Event()

        app = await self._create_app()
        self.http_runner = web.AppRunner(app)
        await self.http_runner.setup()
        site = web.TCPSite(self.http_runner, "0.0.0.0", self.port)
        await site.start()

        self.logger.info(f"🌍 HTTP+WS server listening on 0.0.0.0:{self.port} (WS path={WS_PATH})")
        self.logger.info(f"🔗 Advertised peer URL: {self.advertised_url}")

        # background tasks
        self._tasks.append(asyncio.create_task(self._heartbeat()))
        self._tasks.append(asyncio.create_task(self._hello_task()))
        await self._say_hello()

        await self._shutdown_event.wait()

        # graceful shutdown
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        if self.http_runner:
            await self.http_runner.cleanup()

        self.logger.info("Server stopped")

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

    # ---------------- Helpers: validation/convert ---------------- #
    def _tx_from_any(self, t: Any) -> Transaction:
        if isinstance(t, Transaction):
            return t
        if isinstance(t, dict):
            return Transaction.from_dict(t)
        raise TypeError("Unsupported tx payload")

    def _block_to_network(self, block: Block) -> Dict[str, Any]:
        return block.to_dict()

    def _block_from_network(self, d: Dict[str, Any]) -> Block:
        txs = [
            Transaction.from_dict(t) if not isinstance(t, Transaction) else t
            for t in d.get("transactions", [])
        ]
        b = Block(
            index=d.get("index", 0),
            previous_hash=d.get("previous_hash", d.get("prev_hash", "0")),
            transactions=txs,
            nonce=d.get("nonce", 0),
            miner=d.get("miner", "unknown"),
            difficulty=d.get("difficulty", 1),
            reward=d.get("reward", 0),
            timestamp=d.get("timestamp"),
        )
        b.hash = d.get("hash", b.compute_hash())
        return b

    def _validate_transaction(self, tx: Transaction) -> bool:
        try:
            if hasattr(tx, "verify") and callable(getattr(tx, "verify")):
                if not tx.verify():
                    return False
            elif hasattr(tx, "verify_signature") and callable(getattr(tx, "verify_signature")):
                if not tx.verify_signature():
                    return False
        except Exception:
            return False

        if tx.sender != SYSTEM_SENDER:
            pending_out = 0.0
            for mem_tx in getattr(self.blockchain, "transaction_pool", []):
                if isinstance(mem_tx, Transaction) and mem_tx.sender == tx.sender:
                    pending_out += float(getattr(mem_tx, "amount", 0.0))
            current_bal = float(self.blockchain.balances.get(tx.sender, 0.0))
            if pending_out + float(getattr(tx, "amount", 0.0)) > current_bal + 1e-9:
                return False
        return True

    def _validate_block_against_tip(self, block: Block) -> bool:
        tip = self.blockchain.chain[-1]
        if block.previous_hash != tip.hash:
            return False
        if block.compute_hash() != block.hash:
            return False
        if not block.hash.startswith("0" * max(1, block.difficulty)):
            return False
        for tx in block.transactions:
            if not self._validate_transaction(tx):
                return False
        return True

    def _apply_block_and_clean_pool(self, block: Block, mempool_clean: bool = True):
        for tx in block.transactions:
            amt = float(getattr(tx, "amount", 0.0))
            if tx.sender != SYSTEM_SENDER:
                self.blockchain.balances[tx.sender] = self.blockchain.balances.get(tx.sender, 0.0) - amt
            self.blockchain.balances[tx.recipient] = self.blockchain.balances.get(tx.recipient, 0.0) + amt
        self.blockchain.chain.append(block)
        if mempool_clean:
            mem = self.blockchain.transaction_pool
            keep: List[Transaction] = []
            included: List[Tuple[str, str, float]] = [
                (t.sender, t.recipient, float(t.amount)) for t in block.transactions if t.sender != SYSTEM_SENDER
            ]
            for tx in mem:
                sig = (tx.sender, tx.recipient, float(tx.amount))
                if sig not in included:
                    keep.append(tx)
            self.blockchain.transaction_pool = keep
        self.blockchain._save_chain()
        self.blockchain._save_balances()

    def _is_chain_valid(self, chain: List[Block]) -> bool:
        if not chain:
            return False
        if chain[0].hash != chain[0].compute_hash():
            return False
        for i in range(1, len(chain)):
            a = chain[i - 1]
            b = chain[i]
            if b.previous_hash != a.hash:
                return False
            if b.compute_hash() != b.hash:
                return False
            if not b.hash.startswith("0" * max(1, b.difficulty)):
                return False
            for tx in b.transactions:
                if not self._validate_transaction(tx):
                    return False
        return True


# ---------------- CLI Runner ---------------- #
if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", 5001)))
    parser.add_argument("--wallet", type=str, default="wallet.json")
    parser.add_argument("--data", type=str, default="data")
    parser.add_argument("--peers", nargs="*", default=[])
    parser.add_argument("--log", type=str, default="info")
    args = parser.parse_args()

    level = getattr(logging, args.log.upper(), logging.INFO)
    logging.basicConfig(level=level)

    n = Node(args.port, set(args.peers), args.wallet, data_dir=args.data)
    n.start()
    print(f"🌍 HTTP+WS on :{n.port}  (WS path={WS_PATH})")
    print(f"🔗 Advertised URL: {n.advertised_url}")
    print(f"💳 Address: {n.address}")

    if sys.stdin.isatty():
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
    else:
        # Cloud mode (Render) — no stdin
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            n.shutdown()


# ---------------- BotManager (optional) ---------------- #
class BotManager:
    """Simple bot traffic generator (for testing)."""
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

