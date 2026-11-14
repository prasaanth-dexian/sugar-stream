# Copyright (c) 2026 Dexian India Technologies Private Limited.
# All rights reserved.
#
# This file is proprietary and confidential. Unauthorised copying,
# distribution or use of this file, via any medium, is strictly
# prohibited without the prior written permission of Dexian India
# Technologies Private Limited.

"""Delivery ledger: which readings (collector, epoch, seq) the gateway has stored.

The collector numbers every reading with a `seq` that is never reused within an
epoch, and tells us with each batch the lowest seq it has NOT yet had confirmed
(`min_unsynced_seq`). Every seq below that was confirmed by us earlier, so it
must be in our ranges. A seq below it that we do not have is a confirmed loss,
not a reading still in transit. This is the end-to-end proof of zero loss.

Seqs that were already synced before this gateway first heard from a collector
(for example delivered by an older system) are excluded through `baseline`.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field


def merge_ranges(ranges: list[list[int]], seqs: list[int]) -> list[list[int]]:
    """Union of inclusive [lo, hi] ranges and a list of seqs, merged and sorted."""
    if not seqs:
        return ranges
    new: list[list[int]] = []
    for s in sorted(set(seqs)):
        if new and s == new[-1][1] + 1:
            new[-1][1] = s
        else:
            new.append([s, s])
    merged: list[list[int]] = []
    for lo, hi in sorted(ranges + new):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return merged


def holes(ranges: list[list[int]], start: int, stop: int) -> list[list[int]]:
    """Inclusive ranges in [start, stop) not covered by `ranges`."""
    out: list[list[int]] = []
    cur = start
    for lo, hi in ranges:
        if hi < cur:
            continue
        if lo >= stop:
            break
        if lo > cur:
            out.append([cur, min(lo, stop) - 1])
        cur = max(cur, hi + 1)
        if cur >= stop:
            break
    if cur < stop:
        out.append([cur, stop - 1])
    return out


@dataclass
class CollectorState:
    collector_id: str
    epoch: int
    baseline: int
    ranges: list[list[int]] = field(default_factory=list)
    min_unsynced: int = 0
    max_seq: int = 0
    rows_stored: int = 0
    rows_rejected: int = 0
    last_batch_at: float = 0.0
    reported_missing: int = 0

    def missing(self) -> list[list[int]]:
        return holes(self.ranges, self.baseline, self.min_unsynced)

    def missing_count(self) -> int:
        return sum(hi - lo + 1 for lo, hi in self.missing())

    def backlog(self) -> int:
        """Readings the collector has numbered but we have not received yet."""
        return sum(hi - lo + 1 for lo, hi in holes(self.ranges, self.min_unsynced, self.max_seq + 1))


class Ledger:
    """Durable (SQLite, synchronous=FULL) per-(collector, epoch) delivery state."""

    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS collectors (
                 collector_id TEXT NOT NULL, epoch INTEGER NOT NULL, state TEXT NOT NULL,
                 PRIMARY KEY (collector_id, epoch))"""
        )
        self.states: dict[tuple[str, int], CollectorState] = {}
        for cid, epoch, raw in self._db.execute("SELECT collector_id, epoch, state FROM collectors"):
            self.states[(cid, epoch)] = CollectorState(**json.loads(raw))

    def record(self, collector_id: str, epoch: int, seqs: list[int], rejected: int,
               min_unsynced: int, max_seq: int) -> tuple[CollectorState, list[list[int]]]:
        """Add a stored batch. Returns the state and any NEW confirmed-missing ranges."""
        with self._lock:
            key = (collector_id, epoch)
            st = self.states.get(key)
            if st is None:
                first = min(seqs) if seqs else min_unsynced
                st = CollectorState(collector_id, epoch, baseline=min(first, min_unsynced))
            st.ranges = merge_ranges(st.ranges, seqs)
            st.min_unsynced = max(st.min_unsynced, min_unsynced)
            st.max_seq = max(st.max_seq, max_seq, max(seqs, default=0))
            st.rows_stored += len(seqs) - rejected
            st.rows_rejected += rejected
            st.last_batch_at = time.time()
            missing = st.missing()
            count = sum(hi - lo + 1 for lo, hi in missing)
            new_missing = missing if count > st.reported_missing else []
            st.reported_missing = max(st.reported_missing, count)
            self._db.execute(
                "INSERT OR REPLACE INTO collectors (collector_id, epoch, state) VALUES (?, ?, ?)",
                (collector_id, epoch, json.dumps(st.__dict__)),
            )
            self.states[key] = st
            return st, new_missing

    def close(self) -> None:
        self._db.close()
