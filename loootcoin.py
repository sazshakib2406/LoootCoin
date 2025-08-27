import hashlib
import json
import os
import time
from typing import List, Dict, Any, Optional

SYSTEM_SENDER = "SYSTEM"


class Transaction:
    def __init__(self, sender: str, recipient: str, amount: float,
                 signature: Optional[str] = None, public_key: Optional[str] = None):
        self.sender = sender
        self.recipient = recipient
        self.amount = float(amount)
        # optional fields for future signature schemes
        self.signature = signature
        self.public_key = public_key

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "sender": self.sender,
            "recipient": self.recipient,
            "amount": self.amount,
        }
        if self.signature is not None:
            d["signature"] = self.signature
        if self.public_key is not None:
            d["public_key"] = self.public_key
        return d

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Transaction":
        # be lenient with "receiver" vs "recipient"
        recipient = data.get("recipient", data.get("receiver"))
        return Transaction(
            sender=data["sender"],
            recipient=recipient,
            amount=float(data["amount"]),
            signature=data.get("signature"),
            public_key=data.get("public_key"),
        )

    # Optional placeholder — your node may call verify()/verify_signature() safely
    def verify(self) -> bool:
        # Add real signature verification if your wallet produces signatures
        return True

    def verify_signature(self) -> bool:
        return self.verify()


class Block:
    def __init__(self,
                 index: int,
                 previous_hash: str,
                 transactions: List[Transaction],
                 nonce: int,
                 miner: str,
                 difficulty: int,
                 reward: float,
                 timestamp: Optional[float] = None):
        self.index = index
        self.previous_hash = previous_hash
        self.transactions = transactions
        self.nonce = nonce
        self.miner = miner
        self.difficulty = difficulty
        self.reward = float(reward)
        self.timestamp = timestamp or time.time()
        self.hash = self.compute_hash()

    def compute_hash(self) -> str:
        payload = {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "nonce": self.nonce,
            "miner": self.miner,
            "difficulty": self.difficulty,
            "reward": self.reward,
            "timestamp": self.timestamp,
        }
        s = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "nonce": self.nonce,
            "miner": self.miner,
            "difficulty": self.difficulty,
            "reward": self.reward,
            "timestamp": self.timestamp,
            "hash": self.hash,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Block":
        txs = [Transaction.from_dict(t) for t in data.get("transactions", [])]
        b = Block(
            index=int(data.get("index", 0)),
            previous_hash=data.get("previous_hash", data.get("prev_hash", "0")),
            transactions=txs,
            nonce=int(data.get("nonce", 0)),
            miner=data.get("miner", "unknown"),
            difficulty=int(data.get("difficulty", 1)),
            reward=float(data.get("reward", 0)),
            timestamp=data.get("timestamp"),
        )
        # keep provided hash if present
        b.hash = data.get("hash", b.compute_hash())
        return b


class LoootCoin:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.chain: List[Block] = []
        self.transaction_pool: List[Transaction] = []
        self.balances: Dict[str, float] = {}

        self.difficulty: int = 2  # simple constant difficulty (node checks PoW via prefix zeros)

        self._chain_file = os.path.join(self.data_dir, "chain.json")
        self._balances_file = os.path.join(self.data_dir, "balances.json")

        # Load or create genesis
        if os.path.exists(self._chain_file):
            self._load_chain()
        else:
            self._create_genesis_block()

        if os.path.exists(self._balances_file):
            self._load_balances()
        else:
            self._save_balances()

    # ---------- Persistence ----------
    def _create_genesis_block(self) -> None:
        genesis = Block(
            index=0,
            previous_hash="0",
            transactions=[],
            nonce=0,
            miner=SYSTEM_SENDER,
            difficulty=self.difficulty,
            reward=0.0,
            timestamp=time.time(),
        )
        # For genesis, we don't do PoW; use computed hash
        genesis.hash = genesis.compute_hash()
        self.chain = [genesis]
        self._save_chain()

    def _save_chain(self) -> None:
        with open(self._chain_file, "w", encoding="utf-8") as f:
            json.dump([b.to_dict() for b in self.chain], f, indent=2)

    def _load_chain(self) -> None:
        with open(self._chain_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.chain = [Block.from_dict(b) for b in data]

    def _save_balances(self) -> None:
        with open(self._balances_file, "w", encoding="utf-8") as f:
            json.dump(self.balances, f, indent=2)

    def _load_balances(self) -> None:
        with open(self._balances_file, "r", encoding="utf-8") as f:
            self.balances = {k: float(v) for k, v in json.load(f).items()}

    # ---------- Transactions ----------
    def add_transaction(self, tx: Any) -> bool:
        """
        Accept Transaction (or dict), basic sanity checks, and enqueue into mempool.
        DOES NOT mutate balances here; your node will apply balances when a block is accepted.
        """
        try:
            if isinstance(tx, dict):
                tx = Transaction.from_dict(tx)
            elif not isinstance(tx, Transaction):
                return False

            # Quick value sanity
            if tx.amount <= 0:
                return False

            # Optional: if not SYSTEM, ensure sender likely has enough (soft check)
            if tx.sender != SYSTEM_SENDER:
                # NOTE: Node also guards against mempool overspend; this is a soft check.
                if self.balances.get(tx.sender, 0.0) < tx.amount:
                    return False

            # Optional signature check hooks
            if hasattr(tx, "verify") and callable(getattr(tx, "verify")):
                if not tx.verify():
                    return False

            self.transaction_pool.append(tx)
            return True
        except Exception:
            return False

    # ---------- Mining ----------
    def mine_block(self, miner_address: str) -> Optional[Block]:
        """
        Create a new block with PoW. Block reward = 10 LC distributed equally among all known wallets
        via SYSTEM -> wallet reward transactions (so node's block apply will credit balances).
        """
        if not self.chain:
            self._create_genesis_block()

        index = len(self.chain)
        previous_hash = self.chain[-1].hash

        # Take current mempool snapshot
        txs: List[Transaction] = list(self.transaction_pool)
        self.transaction_pool.clear()

        # --- Reward distribution: 10 LC split equally among wallets ---
        reward_total = 10.0
        wallets = list(self.balances.keys())
        if wallets:
            share = reward_total / float(len(wallets))
            for addr in wallets:
                txs.append(Transaction(SYSTEM_SENDER, addr, share))
        else:
            # bootstrap: if no wallets known yet, grant full reward to miner
            txs.append(Transaction(SYSTEM_SENDER, miner_address, reward_total))

        # PoW loop
        difficulty = self.difficulty
        nonce = 0
        while True:
            candidate = Block(
                index=index,
                previous_hash=previous_hash,
                transactions=txs,
                nonce=nonce,
                miner=miner_address,
                difficulty=difficulty,
                reward=reward_total,
                timestamp=time.time(),
            )
            if candidate.hash.startswith("0" * max(1, difficulty)):
                block = candidate
                break
            nonce += 1

        # Persist chain (balances will be mutated by node when applying the block)
        self.chain.append(block)
        self._save_chain()
        # NOTE: Do NOT save balances here; node updates balances upon block acceptance.
        return block

    # ---------- Supply ----------
    def total_supply(self) -> float:
        """Return the total circulating supply (sum of balances)."""
        return float(sum(self.balances.values()))
