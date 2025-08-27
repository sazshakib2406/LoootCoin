import hashlib, json, os, time, random

class Transaction:
    def __init__(self, sender, recipient, amount, signature=None):
        self.sender = sender
        self.recipient = recipient
        self.amount = amount
        self.signature = signature

    def to_dict(self):
        return {
            "sender": self.sender,
            "recipient": self.recipient,
            "amount": self.amount,
            "signature": self.signature
        }

    @staticmethod
    def from_dict(data):
        return Transaction(
            data.get("sender", ""),
            data.get("recipient", ""),
            data.get("amount", 0),
            data.get("signature")
        )

class Block:
    def __init__(self, index, previous_hash, transactions, nonce, miner, difficulty, reward, timestamp=None):
        self.index = index
        self.previous_hash = previous_hash
        self.transactions = transactions
        self.nonce = nonce
        self.miner = miner
        self.difficulty = difficulty
        self.reward = reward
        self.timestamp = timestamp or time.time()
        self.hash = self.compute_hash()

    def compute_hash(self):
        block_string = json.dumps(self.to_dict(include_hash=False), sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()

    def to_dict(self, include_hash=True):
        return {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "nonce": self.nonce,
            "miner": self.miner,
            "difficulty": self.difficulty,
            "reward": self.reward,
            "timestamp": self.timestamp,
            "hash": self.hash if include_hash else None
        }

    @staticmethod
    def from_dict(data):
        transactions = [Transaction.from_dict(t) for t in data.get("transactions", [])]
        block = Block(
            data.get("index", 0),
            data.get("previous_hash", "0"),
            transactions,
            data.get("nonce", 0),
            data.get("miner", "unknown"),
            data.get("difficulty", 1),
            data.get("reward", 0),
            data.get("timestamp", time.time())
        )
        block.hash = data.get("hash", block.compute_hash())
        return block

class LoootCoin:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.chain_file = os.path.join(data_dir, "chain.json")
        self.balances_file = os.path.join(data_dir, "balances.json")
        self.transaction_pool = []

        self.chain = []
        self.balances = {}

        self._load_chain()
        self._load_balances()

        if not self.chain:
            self.create_genesis_block()

    def create_genesis_block(self):
        genesis = Block(0, "0", [], 0, "genesis", 1, 0)
        self.chain.append(genesis)
        self._save_chain()

    def get_balance(self, address: str) -> float:
        """Return balance for an address, defaulting to 0."""
        return self.balances.get(address, 0.0)



    def add_transaction(self, tx: Transaction):
        if self._valid_transaction(tx):
            self.transaction_pool.append(tx)
            return True
        return False

    def _valid_transaction(self, tx: Transaction):
        if tx.sender == "SYSTEM":
            return True
        if self.balances.get(tx.sender, 0) < tx.amount:
            return False
        return True

       class LoootCoin:
    # ... keep your existing __init__, save/load, etc. ...

    def mine_block(self, miner_address):
        index = len(self.chain)
        previous_hash = self.chain[-1].hash if self.chain else "0"
        reward = 10  # fixed reward per block

        transactions = list(self.transaction_pool)
        self.transaction_pool = []

        # --- distribute reward equally among wallets ---
        total_wallets = len(self.balances) if self.balances else 1
        share = reward / total_wallets

        if self.balances:
            for addr in self.balances.keys():
                self.balances[addr] = self.balances.get(addr, 0) + share
        else:
            # if no wallets exist yet, bootstrap with miner
            self.balances[miner_address] = reward

        difficulty = self._adjust_difficulty()
        nonce = 0
        while True:
            block = Block(index, previous_hash, transactions, nonce, miner_address, difficulty, reward)
            if block.hash.startswith("0" * difficulty):
                break
            nonce += 1

        self.chain.append(block)
        self._save_chain()
        self._save_balances()
        return block

    def total_supply(self) -> float:
        """Return the total circulating supply of LC (sum of all balances)."""
        return sum(self.balances.values())


    def _update_balances(self, transactions):
        for tx in transactions:
            if tx.sender != "SYSTEM":
                self.balances[tx.sender] = self.balances.get(tx.sender, 0) - tx.amount
            self.balances[tx.recipient] = self.balances.get(tx.recipient, 0) + tx.amount

    def _adjust_difficulty(self):
        target_time = 30
        if len(self.chain) < 2:
            return 2
        last_block = self.chain[-1]
        prev_block = self.chain[-2]
        elapsed = last_block.timestamp - prev_block.timestamp
        diff = last_block.difficulty
        if elapsed < target_time / 2:
            return diff + 1
        elif elapsed > target_time * 2 and diff > 1:
            return diff - 1
        return diff

    def _calculate_reward(self, height):
        base = 50
        halvings = height // 100
        reward = base * (0.995 ** halvings)
        return max(1, reward)

    def _save_chain(self):
        with open(self.chain_file, "w") as f:
            json.dump([b.to_dict() for b in self.chain], f)

    def _load_chain(self):
        if not os.path.exists(self.chain_file):
            self.chain = []
            return
        with open(self.chain_file, "r") as f:
            arr = json.load(f)
            self.chain = [Block.from_dict(b) for b in arr]

    def _save_balances(self):
        with open(self.balances_file, "w") as f:
            json.dump(self.balances, f)

    def _load_balances(self):
        if not os.path.exists(self.balances_file):
            self.balances = {}
            return
        with open(self.balances_file, "r") as f:
            self.balances = json.load(f)


