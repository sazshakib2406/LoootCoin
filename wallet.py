# wallet.py
import os, json, base64
from ecdsa import SigningKey, VerifyingKey, SECP256k1, BadSignatureError

# ---- base64 helpers (robust to missing padding) ----
def _b64decode(s: str) -> bytes:
    if isinstance(s, str):
        s_bytes = s.encode("ascii")
    else:
        s_bytes = s
    missing = (-len(s_bytes)) % 4
    if missing:
        s_bytes += b"=" * missing
    return base64.b64decode(s_bytes)

def _b64encode(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")

# ---- key / wallet generation ----
def generate_keypair():
    """Return (private_key_b64, public_key_b64, address)."""
    sk = SigningKey.generate(curve=SECP256k1)
    vk = sk.verifying_key
    priv_b64 = _b64encode(sk.to_string())
    pub_b64  = _b64encode(vk.to_string())
    # simple random address (not derived from pubkey, which is fine for this demo chain)
    address  = _b64encode(os.urandom(20))
    return priv_b64, pub_b64, address

def generate_wallet(filename: str | None = None):
    """Create a wallet dict; if filename provided, save it."""
    priv, pub, addr = generate_keypair()
    w = {"private_key": priv, "public_key": pub, "address": addr}
    if filename:
        with open(filename, "w") as f:
            json.dump(w, f, indent=2)
    return w

def load_wallet(filename="wallet.json"):
    with open(filename, "r") as f:
        return json.load(f)

def load_or_create_wallet(filename="wallet.json"):
    if os.path.exists(filename):
        return load_wallet(filename)
    return generate_wallet(filename)

# ---- signing helpers (consistent message format) ----
def tx_message(sender: str, recipient: str, amount: float) -> str:
    # Use a deterministic canonical string
    return f"{sender}->{recipient}:{float(amount)}"

def sign_message(message: str, private_key_b64: str) -> str:
    sk = SigningKey.from_string(_b64decode(private_key_b64), curve=SECP256k1)
    sig = sk.sign(message.encode("utf-8"))
    return _b64encode(sig)

def verify_signature(message: str, signature_b64: str, public_key_b64: str) -> bool:
    try:
        vk = VerifyingKey.from_string(_b64decode(public_key_b64), curve=SECP256k1)
        sig = _b64decode(signature_b64)
        return vk.verify(sig, message.encode("utf-8"))
    except Exception:
        return False

def sign_tx(tx, private_key_b64: str) -> str:
    """Convenience: sign a Transaction object that has sender/recipient/amount."""
    return sign_message(tx_message(tx.sender, tx.recipient, tx.amount), private_key_b64)
