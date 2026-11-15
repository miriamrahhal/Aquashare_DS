"""Merkle tree over the committed Raft log (RFC 6962 / RFC 9162 layout).

The hash chain (storage.py) lets the three ledger nodes compare their logs, but to check
one record a reader would need the whole chain. A Merkle tree fixes that: the ledger
publishes one root hash for its first N committed entries, and for any entry it can give a
short proof (about log2(N) hashes) that the entry is part of that tree. A farm checks the
proof itself, offline, without trusting the server; changing any byte of any entry changes
the root.

Leaves and inner nodes are hashed with different prefixes (0x00 and 0x01), as in RFC 6962,
so an inner node can never be passed off as a leaf.
"""
import hashlib


def leaf_hash(entry_hash_hex: str) -> bytes:
    return hashlib.sha256(b"\x00" + bytes.fromhex(entry_hash_hex)).digest()


def node_hash(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + left + right).digest()


def _split(n: int) -> int:
    """Largest power of two strictly smaller than n."""
    k = 1
    while k * 2 < n:
        k *= 2
    return k


def root(leaves: list[bytes]) -> bytes:
    n = len(leaves)
    if n == 0:
        return hashlib.sha256(b"").digest()
    if n == 1:
        return leaves[0]
    k = _split(n)
    return node_hash(root(leaves[:k]), root(leaves[k:]))


def proof(leaves: list[bytes], m: int) -> list[bytes]:
    """Audit path for leaf m (0-based) in a tree of len(leaves) leaves."""
    n = len(leaves)
    if n <= 1:
        return []
    k = _split(n)
    if m < k:
        return proof(leaves[:k], m) + [root(leaves[k:])]
    return proof(leaves[k:], m - k) + [root(leaves[:k])]


def verify(leaf: bytes, m: int, size: int, path: list[bytes], expected_root: bytes) -> bool:
    """RFC 9162 section 2.1.3.2: does `path` prove that `leaf` is leaf m of the tree with this root?"""
    if m >= size:
        return False
    fn, sn, r = m, size - 1, leaf
    for p in path:
        if sn == 0:
            return False
        if fn % 2 == 1 or fn == sn:
            r = node_hash(p, r)
            while fn % 2 == 0 and fn != 0:
                fn >>= 1
                sn >>= 1
        else:
            r = node_hash(r, p)
        fn >>= 1
        sn >>= 1
    return sn == 0 and r == expected_root
