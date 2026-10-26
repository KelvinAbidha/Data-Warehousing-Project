"""
ICS 2315 - Data Architecture & Warehousing
Block 2: MongoDB Document Store, Indexing Strategies & Query Optimization

This module handles:
1. Connecting to local MongoDB via pymongo.
2. Ingesting cleaned NSE stock data into a nested BSON document structure.
3. Conducting the Indexing Experiment:
   - Scenario 1: Unindexed Collection Scan (COLLSCAN)
   - Scenario 2: Single-Field Index (IXSCAN on 'code')
   - Scenario 3: Compound Index (IXSCAN on {'code': 1, 'date': -1})
4. Generating Execution Plan Diagnostics (.explain("executionStats")).
5. Running Analytical Aggregation Pipelines (MapReduce equivalent in MongoDB).
"""

import os
import sys
import time
from datetime import datetime
import pandas as pd
import numpy as np
import pymongo
from dotenv import load_dotenv

# Ensure UTF-8 output encoding on Windows PowerShell
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# Load connection settings from .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "coursework_nse")
COLLECTION_NAME = "stocks"


def get_mongo_collection():
    """Connects to MongoDB and returns the stocks collection."""
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[MONGO_DB_NAME]
    return client, db[COLLECTION_NAME]


def clean_val(val):
    """Replaces pandas NaN with Python None for BSON null serialization."""
    if pd.isna(val) or val is np.nan:
        return None
    return val


def ingest_clean_data(csv_path: str = None, batch_size: int = 5000):
    """
    Reads the cleaned NSE CSV and ingests records into MongoDB
    using a nested BSON document model.
    """
    if csv_path is None:
        csv_path = "data/cleaned_nse_stocks_2007_2012.csv" if os.path.exists("data/cleaned_nse_stocks_2007_2012.csv") else "cleaned_nse_stocks_2007_2012.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Cleaned dataset '{csv_path}' not found. Run src/etl.py first.")

    print(f"[Mongo] Loading {csv_path}...")
    df = pd.read_csv(csv_path)

    # Convert date strings to Python datetime objects for native MongoDB ISODate
    df["date"] = pd.to_datetime(df["date"])

    client, collection = get_mongo_collection()

    # Drop existing collection and indexes for a clean benchmark
    collection.drop()
    print(f"[Mongo] Initialized empty collection '{COLLECTION_NAME}' in database '{MONGO_DB_NAME}'.")

    documents = []
    total_rows = len(df)
    start_time = time.time()

    print(f"[Mongo] Transforming and batch-inserting {total_rows:,} documents...")

    for row in df.itertuples(index=False):
        doc = {
            "code": row.code,
            "name": row.name,
            "date": row.date.to_pydatetime(),
            "source_year": int(row.source_year) if not pd.isna(row.source_year) else None,
            "trading": {
                "day_price": clean_val(row.day_price),
                "previous_price": clean_val(row.previous_price),
                "change": clean_val(row.change),
                "change_pct": clean_val(row.change_pct),
                "volume": int(row.volume) if not pd.isna(row.volume) else 0,
                "adjust": clean_val(row.adjust),
            },
            "intraday": {
                "low": clean_val(row.day_low),
                "high": clean_val(row.day_high),
            },
            "yearly_range": {
                "low": clean_val(row.twelve_month_low),
                "high": clean_val(row.twelve_month_high),
            },
        }
        documents.append(doc)

        if len(documents) >= batch_size:
            collection.insert_many(documents, ordered=False)
            documents = []

    if documents:
        collection.insert_many(documents, ordered=False)

    elapsed = time.time() - start_time
    count = collection.count_documents({})
    print(f"[Mongo] Successfully ingested {count:,} documents in {elapsed:.2f} seconds!\n")
    client.close()


def extract_explain_stats(explain_output: dict) -> dict:
    """Extracts key performance indicators from MongoDB explain('executionStats')."""
    stats = explain_output.get("executionStats", {})
    stages = stats.get("executionStages", {})

    # In case of nested stages (e.g. FETCH -> IXSCAN or SORT -> COLLSCAN)
    stage_name = stages.get("stage", "UNKNOWN")
    input_stage = stages.get("inputStage", {})
    if input_stage:
        stage_name = f"{stage_name} -> {input_stage.get('stage', '')}"

    return {
        "execution_time_ms": stats.get("executionTimeMillis", 0),
        "total_docs_examined": stats.get("totalDocsExamined", 0),
        "total_keys_examined": stats.get("totalKeysExamined", 0),
        "n_returned": stats.get("nReturned", 0),
        "winning_stage": stage_name,
    }


def run_indexing_experiment():
    """
    Executes the 3-phase Indexing Experiment:
    Query: Find all 2012 trades for 'SCOM' (Safaricom) with volume > 1,000,000,
           sorted by date descending.
    """
    client, collection = get_mongo_collection()

    test_query = {
        "code": "SCOM",
        "date": {
            "$gte": datetime(2012, 1, 1),
            "$lte": datetime(2012, 12, 31, 23, 59, 59),
        },
        "trading.volume": {"$gt": 1000000},
    }
    sort_criteria = [("date", pymongo.DESCENDING)]

    results = []

    print("=" * 70)
    print("[EXPERIMENT] THE MONGODB INDEXING BENCHMARK")
    print("=" * 70)
    print("Target Query:")
    print("  * Stock: 'SCOM' (Safaricom)")
    print("  * Date Range: 2012-01-01 to 2012-12-31")
    print("  * Filter: trading.volume > 1,000,000 shares")
    print("  * Sort: date DESC\n")

    # ---------------------------------------------------------
    # Scenario 1: NO INDEX (Baseline Collection Scan)
    # ---------------------------------------------------------
    collection.drop_indexes()
    print("[Phase 1] Running query with NO INDEXES (Baseline)...")
    exp_no_idx = collection.find(test_query).sort(sort_criteria).explain()
    stats_no_idx = extract_explain_stats(exp_no_idx)
    stats_no_idx["scenario"] = "Scenario 1: No Index (COLLSCAN)"
    results.append(stats_no_idx)

    # ---------------------------------------------------------
    # Scenario 2: SINGLE-FIELD INDEX on {"code": 1}
    # ---------------------------------------------------------
    print("[Phase 2] Creating Single-Field Index on {'code': 1}...")
    collection.create_index([("code", pymongo.ASCENDING)], name="idx_code")
    exp_single_idx = collection.find(test_query).sort(sort_criteria).explain()
    stats_single_idx = extract_explain_stats(exp_single_idx)
    stats_single_idx["scenario"] = "Scenario 2: Single Index {'code': 1}"
    results.append(stats_single_idx)

    # ---------------------------------------------------------
    # Scenario 3: COMPOUND INDEX on {"code": 1, "date": -1}
    # ---------------------------------------------------------
    print("[Phase 3] Creating Compound Index on {'code': 1, 'date': -1}...")
    collection.create_index(
        [("code", pymongo.ASCENDING), ("date", pymongo.DESCENDING)],
        name="idx_code_date_desc",
    )
    exp_compound_idx = collection.find(test_query).sort(sort_criteria).explain()
    stats_compound_idx = extract_explain_stats(exp_compound_idx)
    stats_compound_idx["scenario"] = "Scenario 3: Compound {'code': 1, 'date': -1}"
    results.append(stats_compound_idx)

    # ---------------------------------------------------------
    # Print Benchmark Summary Table
    # ---------------------------------------------------------
    summary_df = pd.DataFrame(results)[
        [
            "scenario",
            "winning_stage",
            "total_docs_examined",
            "total_keys_examined",
            "n_returned",
            "execution_time_ms",
        ]
    ]

    print("\n" + "=" * 80)
    print("[RESULTS] BENCHMARK EXECUTION STATS (For Coursework Report)")
    print("=" * 80)
    print(summary_df.to_string(index=False))
    print("=" * 80 + "\n")

    client.close()
    return summary_df


def run_aggregations():
    """
    Runs multi-stage aggregation pipelines (MapReduce equivalent in MongoDB).
    """
    client, collection = get_mongo_collection()
    print("=" * 70)
    print("[ANALYTICS] AGGREGATION PIPELINES (MapReduce Equivalent)")
    print("=" * 70)

    # Pipeline 1: Top 5 Traded Stocks by Cumulative Volume across 2007-2012
    pipeline_top_volume = [
        {"$group": {
            "_id": "$code",
            "company_name": {"$first": "$name"},
            "total_volume_traded": {"$sum": "$trading.volume"},
            "avg_price": {"$avg": "$trading.day_price"},
            "trade_days_recorded": {"$sum": 1}
        }},
        {"$sort": {"total_volume_traded": -1}},
        {"$limit": 5}
    ]

    print("\n1. Top 5 Most Traded Stocks by Total Volume (2007 - 2012):")
    top_stocks = list(collection.aggregate(pipeline_top_volume))
    for rank, stock in enumerate(top_stocks, 1):
        print(f"  {rank}. [{stock['_id']}] {stock['company_name']}: "
              f"Volume = {stock['total_volume_traded']:,} shares | "
              f"Avg Price = KES {stock['avg_price']:.2f} ({stock['trade_days_recorded']} trading days)")

    # Pipeline 2: Annual Market Volume Growth
    pipeline_annual_volume = [
        {"$match": {"source_year": {"$ne": None}}},
        {"$group": {
            "_id": "$source_year",
            "total_market_volume": {"$sum": "$trading.volume"},
            "total_trades": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    print("\n2. Annual Market Trading Activity:")
    annual_summary = list(collection.aggregate(pipeline_annual_volume))
    for yr in annual_summary:
        print(f"  - Year {yr['_id']}: {yr['total_market_volume']:,} shares traded across {yr['total_trades']:,} recorded positions")

    print("\n" + "=" * 70 + "\n")
    client.close()


if __name__ == "__main__":
    # 1. Ingest Data
    ingest_clean_data()

    # 2. Run Indexing Experiment
    run_indexing_experiment()

    # 3. Run Aggregation Analytics
    run_aggregations()
