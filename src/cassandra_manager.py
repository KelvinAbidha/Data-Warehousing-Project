"""
ICS 2315 - Data Architecture & Warehousing
Block 3: Apache Cassandra - Distributed Architecture & Query-Driven Modeling

This script handles:
1. Connecting to local Cassandra running in Docker via cassandra-driver.
2. Creating the Keyspace ('coursework_nse') with SimpleStrategy replication.
3. Creating Query-Driven Tables:
   - Table 1: stock_history_by_ticker (Optimized for ticker time-series)
   - Table 2: daily_market_snapshot (Optimized for monthly market slices)
4. Batch concurrent ingestion of 81,504 records from cleaned_nse_stocks_2007_2012.csv.
5. Demonstrating Query Patterns:
   - Point query (single partition, single row).
   - Time-series range slice (single partition, sequential disk read).
6. Conducting the "Why Cassandra Rejects Scans" architectural experiment.
"""

import os
import sys
import time
from datetime import datetime
import pandas as pd
import numpy as np

# Ensure asyncore compatibility on Python 3.12+
try:
    import pyasyncore
except ImportError:
    pass

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra import InvalidRequest
from dotenv import load_dotenv

# Ensure UTF-8 output encoding on Windows PowerShell
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# Load environment configuration
load_dotenv()
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "coursework_nse")


def get_cassandra_session():
    """Connects to the Cassandra cluster and returns cluster and session."""
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    return cluster, session


def setup_schema(session):
    """
    Creates the keyspace and the two query-driven tables.
    """
    print("[SCHEMA] Setting up Keyspace and Tables...")

    # 1. Create Keyspace
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)
    session.set_keyspace(KEYSPACE)
    print(f"  * Keyspace '{KEYSPACE}' active.")

    # 2. Table 1: Optimized for Ticker Historical Time-Series
    # Partition Key: (code) -> All rows for a ticker reside on the same cluster node.
    # Clustering Column: date DESC -> Physically sorted on disk descending by date.
    session.execute("""
    CREATE TABLE IF NOT EXISTS stock_history_by_ticker (
        code text,
        date date,
        name text,
        day_price double,
        previous_price double,
        day_low double,
        day_high double,
        volume bigint,
        change double,
        change_pct double,
        PRIMARY KEY ((code), date)
    ) WITH CLUSTERING ORDER BY (date DESC);
    """)
    print("  * Table 'stock_history_by_ticker' verified.")

    # 3. Table 2: Optimized for Monthly Market-Wide Snapshots
    # Partition Key: (year, month) -> Prevents partitions from unbounded growth.
    # Clustering Columns: date DESC, code ASC -> Fast daily slices.
    session.execute("""
    CREATE TABLE IF NOT EXISTS daily_market_snapshot (
        year int,
        month int,
        date date,
        code text,
        name text,
        day_price double,
        volume bigint,
        change_pct double,
        PRIMARY KEY ((year, month), date, code)
    ) WITH CLUSTERING ORDER BY (date DESC, code ASC);
    """)
    print("  * Table 'daily_market_snapshot' verified.\n")


def clean_num(val, default=None):
    """Safely converts pandas NaN into None for CQL execution."""
    if pd.isna(val) or val is np.nan:
        return default
    return float(val)


def ingest_data(session, csv_path: str = None):
    """
    Ingests 81,504 records concurrently into both Cassandra tables.
    """
    if csv_path is None:
        csv_path = "data/cleaned_nse_stocks_2007_2012.csv" if os.path.exists("data/cleaned_nse_stocks_2007_2012.csv") else "cleaned_nse_stocks_2007_2012.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Cleaned dataset '{csv_path}' not found. Run src/etl.py first.")

    print(f"[INGESTION] Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])

    total_records = len(df)
    print(f"[INGESTION] Preparing concurrent inserts for {total_records:,} records...")

    # Prepare statements once for compiled query execution on the cluster
    stmt_ticker = session.prepare("""
    INSERT INTO stock_history_by_ticker (
        code, date, name, day_price, previous_price, day_low, day_high, volume, change, change_pct
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_snapshot = session.prepare("""
    INSERT INTO daily_market_snapshot (
        year, month, date, code, name, day_price, volume, change_pct
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    params_ticker = []
    params_snapshot = []

    for row in df.itertuples(index=False):
        c_date = row.date.date()  # python datetime.date
        c_year = row.date.year
        c_month = row.date.month

        day_price = clean_num(row.day_price)
        prev_price = clean_num(row.previous_price)
        day_low = clean_num(row.day_low)
        day_high = clean_num(row.day_high)
        change = clean_num(row.change)
        change_pct = clean_num(row.change_pct)
        volume = int(row.volume) if not pd.isna(row.volume) else 0

        params_ticker.append((
            str(row.code),
            c_date,
            str(row.name),
            day_price,
            prev_price,
            day_low,
            day_high,
            volume,
            change,
            change_pct
        ))

        params_snapshot.append((
            c_year,
            c_month,
            c_date,
            str(row.code),
            str(row.name),
            day_price,
            volume,
            change_pct
        ))

    # Ingest into Table 1 concurrently in chunks
    chunk_size = 5000
    start_time = time.time()
    print(f"[INGESTION] Loading into 'stock_history_by_ticker' (concurrency=50)...")
    for i in range(0, total_records, chunk_size):
        chunk = params_ticker[i:i + chunk_size]
        execute_concurrent_with_args(session, stmt_ticker, chunk, concurrency=50)

    elapsed_t1 = time.time() - start_time
    print(f"  * Table 1 populated in {elapsed_t1:.2f} seconds.")

    # Ingest into Table 2 concurrently in chunks
    start_time = time.time()
    print(f"[INGESTION] Loading into 'daily_market_snapshot' (concurrency=50)...")
    for i in range(0, total_records, chunk_size):
        chunk = params_snapshot[i:i + chunk_size]
        execute_concurrent_with_args(session, stmt_snapshot, chunk, concurrency=50)

    elapsed_t2 = time.time() - start_time
    print(f"  * Table 2 populated in {elapsed_t2:.2f} seconds.")
    print(f"[INGESTION] Complete! Both tables synchronized with {total_records:,} records.\n")


def demonstrate_queries(session):
    """
    Runs target queries demonstrating single-partition time-series slices.
    """
    print("=" * 70)
    print("[QUERY DEMO] FAST SINGLE-PARTITION TIME-SERIES SLICES")
    print("=" * 70)

    # Query 1: Retrieve latest 5 trading days for Safaricom ('SCOM')
    print("1. Query: 'Latest 5 trading days for Safaricom (SCOM)'")
    query_scom = """
    SELECT code, date, day_price, volume, change_pct 
    FROM stock_history_by_ticker 
    WHERE code = 'SCOM' 
    LIMIT 5;
    """
    t0 = time.time()
    rows = session.execute(query_scom)
    latency_ms = (time.time() - t0) * 1000

    for r in rows:
        print(f"   [{r.code}] Date: {r.date} | Price: KES {r.day_price:.2f} | Volume: {r.volume:,} | Change: {r.change_pct}%")
    print(f"   --> Query executed in {latency_ms:.2f} ms (Single Partition Read)\n")

    # Query 2: Time-Series Range Query (October 2012 for Equity Bank 'EQTY')
    print("2. Query: 'Equity Bank (EQTY) price range in October 2012'")
    query_eqty = """
    SELECT code, date, day_price, day_high, day_low, volume
    FROM stock_history_by_ticker
    WHERE code = 'EQTY'
      AND date >= '2012-10-01' AND date <= '2012-10-15';
    """
    t0 = time.time()
    rows = session.execute(query_eqty)
    latency_ms = (time.time() - t0) * 1000

    row_list = list(rows)
    for r in row_list[:3]:
        print(f"   [{r.code}] Date: {r.date} | Close: KES {r.day_price:.2f} (High: {r.day_high}, Low: {r.day_low}) | Vol: {r.volume:,}")
    print(f"   ... ({len(row_list)} trading days retrieved in {latency_ms:.2f} ms)\n")

    # Query 3: Monthly Market Snapshot (All stocks on 2012-12-31)
    print("3. Query: 'Market Snapshot on 2012-12-31 (Year=2012, Month=12)'")
    query_snap = """
    SELECT date, code, name, day_price, volume
    FROM daily_market_snapshot
    WHERE year = 2012 AND month = 12 AND date = '2012-12-31'
    LIMIT 5;
    """
    t0 = time.time()
    rows = session.execute(query_snap)
    latency_ms = (time.time() - t0) * 1000

    for r in rows:
        print(f"   Date: {r.date} | [{r.code}] {r.name}: KES {r.day_price} | Vol: {r.volume:,}")
    print(f"   --> Query executed in {latency_ms:.2f} ms\n")


def demonstrate_architectural_experiment(session):
    """
    Demonstrates why Cassandra strictly rejects non-partitioned full scans
    by attempting a query without the Partition Key.
    """
    print("=" * 70)
    print("[EXPERIMENT] WHY CASSANDRA GUARDS AGAINST FULL CLUSTER SCANS")
    print("=" * 70)
    print("Attempting to query without specifying Partition Key 'code':")
    print("  Query: SELECT * FROM stock_history_by_ticker WHERE day_price > 100;\n")

    illegal_cql = "SELECT * FROM stock_history_by_ticker WHERE day_price > 100;"

    try:
        session.execute(illegal_cql)
        print("  Query succeeded (unexpected)!")
    except InvalidRequest as exc:
        print("  [BLOCKED BY CASSANDRA AS EXPECTED!]")
        print(f"  Exact Server Error: {exc}\n")
        print("  Database Architectural Principle:")
        print("  * In Cassandra, a table cannot be searched by an un-indexed column without a Partition Key.")
        print("  * Doing so would force Cassandra to broadcast the query to EVERY node in the cluster,")
        print("    violating distributed linear scalability.")
        print("  * To override this, Cassandra forces you to explicitly add 'ALLOW FILTERING',")
        print("    warning you of unpredictable latency at scale.\n")

    # Running with ALLOW FILTERING to prove the contrast
    print("Executing with explicit 'ALLOW FILTERING':")
    override_cql = "SELECT code, date, day_price FROM stock_history_by_ticker WHERE day_price > 100 LIMIT 3 ALLOW FILTERING;"
    t0 = time.time()
    rows = session.execute(override_cql)
    latency_ms = (time.time() - t0) * 1000

    for r in rows:
        print(f"  Found: [{r.code}] on {r.date} with price KES {r.day_price}")
    print(f"  Executed with cluster warning in {latency_ms:.2f} ms.\n")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    cluster, session = get_cassandra_session()
    try:
        # 1. Setup Keyspace & Tables
        setup_schema(session)

        # 2. Ingest Data
        ingest_data(session)

        # 3. Demonstrate Fast Query Patterns
        demonstrate_queries(session)

        # 4. Demonstrate Architectural Scan Experiment
        demonstrate_architectural_experiment(session)
    finally:
        cluster.shutdown()
        print("[CASSANDRA] Session closed.")
