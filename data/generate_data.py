"""
generate_data.py
-----------------
Generates synthetic AML/transaction-monitoring data to simulate a real
banking source feed landing in a data lake, one CSV batch per day
(mimics an incremental daily load pattern you would see with Auto Loader
in a real Databricks pipeline).

Two suspicious patterns are deliberately injected so the Gold-layer
detection logic in 03_gold_aggregates.py has real signal to catch:

  1. STRUCTURING  - a sender splits a transfer into several transactions,
     each just under the 10,000 EUR reporting threshold, within a
     24h window (classic AML "smurfing" pattern).
  2. SANCTIONS HIT - a transaction involves a party whose name appears
     (exactly or as a near-match) on the synthetic sanctions watchlist.

Run:
    python generate_data.py

Output:
    data/raw/batch_date=YYYY-MM-DD/transactions.csv   (3 daily batches)
    data/raw/watchlist.csv
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    # Minimal fallback so the script still runs without the `faker` package.
    class _FallbackFaker:
        _first = ["John", "Anna", "Marek", "Elena", "Piotr", "Sofia", "Lukas", "Maria", "Ivan", "Nina"]
        _last = ["Kowalski", "Novak", "Petrov", "Muller", "Dubois", "Rossi", "Silva", "Nowak", "Schmidt", "Lopez"]
        def name(self):
            return f"{random.choice(self._first)} {random.choice(self._last)}"
    fake = _FallbackFaker()

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "raw")
COUNTRIES = ["PL", "FR", "DE", "NL", "GB", "US", "CH", "LU", "CY", "AE", "RU", "IR"]
HIGH_RISK_COUNTRIES = {"RU", "IR", "AE"}
CHANNELS = ["WIRE", "CARD", "MOBILE", "CASH"]
TX_TYPES = ["TRANSFER", "WITHDRAWAL", "DEPOSIT"]
SOURCE_SYSTEMS = ["CORE_BANKING", "CARDS", "MOBILE_APP"]
CURRENCIES = ["EUR", "USD", "GBP", "PLN"]

REPORTING_THRESHOLD = 10000

# A handful of "watchlisted" names we will deliberately reuse for some
# transactions further down so the sanctions-screening notebook has
# genuine matches to find.
WATCHLIST_NAMES = [
    ("Viktor Orlenko", "RU", "OFAC-SIM", "HIGH"),
    ("Amara Haidari", "IR", "EU-SIM", "HIGH"),
    ("Karim Al-Sayed", "AE", "UN-SIM", "MEDIUM"),
    ("Elena Vasnetsova", "RU", "OFAC-SIM", "HIGH"),
]


def make_account_id():
    return "ACC" + str(random.randint(10_000_000, 99_999_999))


def normal_transaction(tx_date):
    amount = round(random.uniform(20, 10_500), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "transaction_date": tx_date.isoformat(sep=" "),
        "sender_account_id": make_account_id(),
        "sender_name": fake.name(),
        "sender_country": random.choice(COUNTRIES),
        "receiver_account_id": make_account_id(),
        "receiver_name": fake.name(),
        "receiver_country": random.choice(COUNTRIES),
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "channel": random.choice(CHANNELS),
        "transaction_type": random.choice(TX_TYPES),
        "source_system": random.choice(SOURCE_SYSTEMS),
    }


def structuring_batch(sender_account, sender_name, base_date):
    """3-4 transactions from the same sender, each just under the
    reporting threshold, spread over a few hours -> should be flagged
    by the structuring detector in the gold layer."""
    rows = []
    n = random.randint(3, 4)
    for i in range(n):
        tx_date = base_date + timedelta(hours=random.uniform(0, 20))
        rows.append({
            "transaction_id": str(uuid.uuid4()),
            "transaction_date": tx_date.isoformat(sep=" "),
            "sender_account_id": sender_account,
            "sender_name": sender_name,
            "sender_country": random.choice(list(COUNTRIES)),
            "receiver_account_id": make_account_id(),
            "receiver_name": fake.name(),
            "receiver_country": random.choice(COUNTRIES),
            "amount": round(random.uniform(9200, 9950), 2),
            "currency": "EUR",
            "channel": "WIRE",
            "transaction_type": "TRANSFER",
            "source_system": "CORE_BANKING",
        })
    return rows


def sanctions_hit_transaction(tx_date):
    name, country, _, _ = random.choice(WATCHLIST_NAMES)
    row = normal_transaction(tx_date)
    if random.random() < 0.5:
        row["sender_name"] = name
        row["sender_country"] = country
    else:
        row["receiver_name"] = name
        row["receiver_country"] = country
    return row


def write_watchlist():
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "watchlist.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["watchlist_name", "country", "list_source", "risk_level"])
        for name, country, source, risk in WATCHLIST_NAMES:
            writer.writerow([name, country, source, risk])
    print(f"wrote {path}")


def write_batch(batch_date):
    day_dir = os.path.join(OUT_DIR, f"batch_date={batch_date.date().isoformat()}")
    os.makedirs(day_dir, exist_ok=True)
    path = os.path.join(day_dir, "transactions.csv")

    rows = []
    for _ in range(400):
        tx_time = batch_date + timedelta(seconds=random.randint(0, 86_399))
        rows.append(normal_transaction(tx_time))

    # inject 2 structuring rings per day
    for _ in range(2):
        sender_account = make_account_id()
        sender_name = fake.name()
        rows.extend(structuring_batch(sender_account, sender_name, batch_date))

    # inject 3 sanctions-list hits per day
    for _ in range(3):
        tx_time = batch_date + timedelta(seconds=random.randint(0, 86_399))
        rows.append(sanctions_hit_transaction(tx_time))

    # inject a handful of exact-duplicate rows to exercise the
    # dedup logic in the silver layer
    for _ in range(4):
        rows.append(dict(random.choice(rows)))

    random.shuffle(rows)

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {path} ({len(rows)} rows)")


if __name__ == "__main__":
    write_watchlist()
    today = datetime.combine(datetime.today().date(), datetime.min.time())
    for offset in range(3, 0, -1):
        write_batch(today - timedelta(days=offset))
    print("Done. Reporting threshold used downstream:", REPORTING_THRESHOLD)
