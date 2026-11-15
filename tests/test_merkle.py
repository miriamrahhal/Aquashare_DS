"""Merkle tree tests: every proof verifies, any change breaks it, and the ledger API serves proofs."""
import hashlib
import json
import random

from ledger_node import merkle
from ledger_node.storage import Entry, MemoryStorage


def leaves(n):
    return [merkle.leaf_hash(hashlib.sha256(str(i).encode()).hexdigest()) for i in range(n)]


def test_every_leaf_of_every_tree_size_verifies():
    for n in range(1, 40):
        ls = leaves(n)
        r = merkle.root(ls)
        for m in range(n):
            assert merkle.verify(ls[m], m, n, merkle.proof(ls, m), r)


def test_proof_is_short():
    ls = leaves(10_000)
    assert len(merkle.proof(ls, 1234)) <= 14                  # about log2(10 000)


def test_changed_leaf_wrong_position_or_wrong_root_fail():
    ls, rng = leaves(37), random.Random(1)
    r = merkle.root(ls)
    for _ in range(50):
        m = rng.randrange(37)
        path = merkle.proof(ls, m)
        assert not merkle.verify(merkle.leaf_hash("ff" * 32), m, 37, path, r)          # other content
        assert not merkle.verify(ls[m], (m + 1) % 37, 37, path, r)                     # other position
        assert not merkle.verify(ls[m], m, 37, path, merkle.root(leaves(36)))          # other tree


def test_root_changes_when_any_entry_changes():
    ls = leaves(20)
    before = merkle.root(ls)
    for i in range(20):
        changed = list(ls)
        changed[i] = merkle.leaf_hash("00" * 32)
        assert merkle.root(changed) != before


def test_proof_over_a_real_log_and_the_independent_verifier():
    from scripts.verify_proof import entry_hash, fold, sha
    s = MemoryStorage()
    s.append([Entry(i, 1, json.dumps({"type": "transfer", "transfer_id": f"t{i}", "amount_l": i * 10}))
              for i in range(1, 26)])
    ls = [merkle.leaf_hash(e.entry_hash) for e in s.log]
    r = merkle.root(ls)
    e = s.entry(17)
    cmd = json.loads(e.command)
    assert entry_hash(e.prev_hash, e.index, e.term, cmd) == e.entry_hash             # verifier agrees with ledger
    path = merkle.proof(ls, 16)
    assert fold(sha(b"\x00" + bytes.fromhex(e.entry_hash)), 16, 25, path) == r
    cmd["amount_l"] = 9999                                                            # tampered amount
    assert fold(sha(b"\x00" + bytes.fromhex(entry_hash(e.prev_hash, e.index, e.term, cmd))), 16, 25, path) != r
