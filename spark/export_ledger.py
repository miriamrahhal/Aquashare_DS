"""Export the committed water records from one ledger database to CSV files for Spark.

  python spark/export_ledger.py                    (reads ledger-1's database, writes spark/data/)

Only applied records are exported (refused commands are not in these tables), so Spark
works on exactly the data the ledger agreed on.
"""
import csv
import os
import sys
from pathlib import Path

import psycopg

DSN = os.getenv("LEDGER_DSN", "postgresql://aquashare:aquashare_dev@localhost:5441/ledger")
OUT = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "data")
QUERIES = {
    "daily_limits": "SELECT day, limit_l FROM daily_limits ORDER BY day",
    "allocations": "SELECT day, farm_id, amount_l FROM allocations ORDER BY day, farm_id",
    "consumption": "SELECT day, farm_id, amount_l, log_index FROM consumption_records ORDER BY log_index",
    "transfers": "SELECT day, sender, recipient, amount_l, status FROM transfers ORDER BY log_index",
}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(DSN) as conn:
        for name, sql in QUERIES.items():
            cur = conn.execute(sql)
            cols = [c.name for c in cur.description]
            rows = cur.fetchall()
            with open(OUT / f"{name}.csv", "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(cols)
                w.writerows(rows)
            print(f"{name:13} {len(rows):>7} rows -> {OUT / (name + '.csv')}")


if __name__ == "__main__":
    main()
