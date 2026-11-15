"""Append-only local event journal, stored as JSON Lines.

Every change to a farm's wallet is written here first, and the wallet is rebuilt
from this file on restart, so a crash never loses or double-counts water.
"""
import json
import os
import threading
from pathlib import Path


class Journal:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self._lock = threading.Lock()

    def append(self, event: dict) -> None:
        """Write one event and force it to disk before returning."""
        line = json.dumps(event, separators=(",", ":")) + "\n"
        with self._lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def read_all(self) -> list[dict]:
        """Return every complete event. A last line cut short by a crash is ignored."""
        events = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    break   # only the final line can be partial: an append was interrupted
        return events