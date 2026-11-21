"""Create a SIMULATED irrigation season in the same CSV format as export_ledger.py.

  python spark/simulate_season.py --days 120 --out spark/data_simulated

The live demo only produces a few days of records. To show what the batch analysis gives
over a whole season, this script generates realistic synthetic data: 4 farms with different
crops, hot and cool days, a dry spell, and farms that help each other with transfers.
It is clearly labelled as simulated in the report; it never touches the real ledger.
"""
import argparse
import csv
import datetime as dt
import math
import random
from pathlib import Path

FARMS = {  # share of the daily limit, how thirsty the crop is
    "farm-a": (0.30, 1.10), "farm-b": (0.25, 0.80), "farm-c": (0.225, 1.00), "farm-d": (0.20, 0.70)}


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--days", type=int, default=120)
    a.add_argument("--start", default="2026-05-01")
    a.add_argument("--out", default=str(Path(__file__).parent / "data_simulated"))
    a.add_argument("--seed", type=int, default=7)
    args = a.parse_args()
    rnd = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    limits, allocs, uses, transfers = [], [], [], []
    start = dt.date.fromisoformat(args.start)
    index = 0
    for d in range(args.days):
        day = start + dt.timedelta(days=d)
        heat = 1 + 0.25 * math.sin(2 * math.pi * (d - 30) / 365 * 2) + (0.25 if 60 <= d < 75 else 0)  # dry spell
        limit = int(20000 * (0.85 if 60 <= d < 75 else 1.0))           # less water during the dry spell
        limits.append((day, limit))
        share = {f: int(limit * s) for f, (s, _) in FARMS.items()}
        for f, v in share.items():
            allocs.append((day, f, v))
        left = dict(share)
        # transfers in the morning: farms that need less help farms that need more
        need = {f: share[f] * thirst * heat * rnd.uniform(0.85, 1.1) for f, (_, thirst) in FARMS.items()}
        for giver in sorted(need, key=lambda f: need[f] - share[f]):
            spare = share[giver] - need[giver]
            if spare < 300:
                break
            taker = max(need, key=lambda f: need[f] - left[f])
            if taker == giver or need[taker] <= left[taker]:
                continue
            amount = int(min(spare * 0.6, need[taker] - left[taker]))
            if amount >= 100:
                left[giver] -= amount
                left[taker] += amount
                transfers.append((day, giver, taker, amount, "committed"))
        # consumption: many small irrigation events, never more than the farm has left
        for f in FARMS:
            target = min(need[f], left[f])
            used = 0
            while used < target - 50:
                amount = int(min(rnd.uniform(150, 450), target - used))
                if amount <= 0:
                    break
                index += 1
                uses.append((day, f, amount, index))
                used += amount
    write(out / "daily_limits.csv", ("day", "limit_l"), limits)
    write(out / "allocations.csv", ("day", "farm_id", "amount_l"), allocs)
    write(out / "consumption.csv", ("day", "farm_id", "amount_l", "log_index"), uses)
    write(out / "transfers.csv", ("day", "sender", "recipient", "amount_l", "status"), transfers)
    print(f"simulated {args.days} days: {len(uses)} irrigation events, {len(transfers)} transfers -> {out}")


def write(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


if __name__ == "__main__":
    main()
