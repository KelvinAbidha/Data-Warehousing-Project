"""
ICS 2315 - Data Architecture & Warehousing
ETL Pipeline: Clean and Consolidate Nairobi Securities Exchange (NSE) Stock Market Data (2007-2012)

This script performs the Extract-Transform-Load (ETL) process:
1. Extract: Reads 6 yearly CSV files (2007 - 2012).
2. Transform:
   - Replaces '-' dash placeholders with NaN.
   - Cleans numeric fields by removing thousands-separator commas (e.g. '7,800' -> 7800.0).
   - Strips '%' from percentage columns and casts to float.
   - Parses dates into standardized datetime objects (YYYY-MM-DD).
   - Standardizes column names into snake_case.
3. Load: Saves the consolidated, clean dataset to 'cleaned_nse_stocks_2007_2012.csv'
   and provides a reusable Python loader function.
"""

import os
import glob
import pandas as pd
import numpy as np


def clean_numeric_series(series: pd.Series) -> pd.Series:
    """
    Cleans a series that contains numbers formatted as strings with commas or dashes.
    Example: '7,800' -> 7800.0, '-' -> NaN
    """
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"-": np.nan, "": np.nan, "None": np.nan, "nan": np.nan})
        .astype(float)
    )


def clean_percentage_series(series: pd.Series) -> pd.Series:
    """
    Cleans percentage strings: '1.50%' -> 1.50, '-' -> NaN
    """
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"-": np.nan, "": np.nan, "None": np.nan, "nan": np.nan})
        .astype(float)
    )


def run_etl(data_dir: str = None) -> pd.DataFrame:
    """
    Finds and processes all NSE stock CSV files from 2007 to 2012.
    """
    if data_dir is None:
        data_dir = "data" if os.path.exists("data") else "."
    csv_pattern = os.path.join(data_dir, "NSE_data_all_stocks_20*.csv")
    files = sorted(glob.glob(csv_pattern))
    
    if not files:
        raise FileNotFoundError(f"No NSE CSV files found matching '{csv_pattern}'.")

    print(f"[ETL] Found {len(files)} files to extract:")
    for f in files:
        print(f"  - {os.path.basename(f)}")

    dfs = []
    for filepath in files:
        year_str = os.path.basename(filepath).split("_")[-1].replace(".csv", "")
        print(f"[ETL] Reading {os.path.basename(filepath)}...")
        df = pd.read_csv(filepath, low_memory=False)
        
        # Normalize column names in this individual file
        # Handles 2008 having 'Date' and 'Adjusted' while other years have 'DATE' and 'Adjust'
        col_rename = {}
        for c in df.columns:
            c_clean = c.strip().lower()
            if c_clean == "date":
                col_rename[c] = "date"
            elif c_clean == "code":
                col_rename[c] = "code"
            elif c_clean == "name":
                col_rename[c] = "name"
            elif c_clean in ["12m low", "12m_low"]:
                col_rename[c] = "twelve_month_low"
            elif c_clean in ["12m high", "12m_high"]:
                col_rename[c] = "twelve_month_high"
            elif c_clean in ["day low", "day_low"]:
                col_rename[c] = "day_low"
            elif c_clean in ["day high", "day_high"]:
                col_rename[c] = "day_high"
            elif c_clean in ["day price", "day_price"]:
                col_rename[c] = "day_price"
            elif c_clean in ["previous", "previous_price"]:
                col_rename[c] = "previous_price"
            elif c_clean == "change":
                col_rename[c] = "change"
            elif c_clean in ["change%", "change_pct"]:
                col_rename[c] = "change_pct"
            elif c_clean == "volume":
                col_rename[c] = "volume"
            elif c_clean in ["adjust", "adjusted"]:
                col_rename[c] = "adjust"
        
        df.rename(columns=col_rename, inplace=True)
        df["source_year"] = int(year_str) if year_str.isdigit() else np.nan
        dfs.append(df)

    # 1. Consolidate into single DataFrame
    raw_df = pd.concat(dfs, ignore_index=True)
    print(f"\n[ETL] Total raw records extracted: {len(raw_df):,}")

    # 3. Clean Text Identifiers
    raw_df["code"] = raw_df["code"].astype(str).str.strip()
    raw_df["name"] = raw_df["name"].astype(str).str.strip()

    # Drop any ghost rows where ticker code is missing or header re-appeared
    raw_df = raw_df[raw_df["code"].str.upper() != "CODE"].dropna(subset=["code", "date"])

    # 4. Standardize Date format to ISO datetime
    # Dates are in M/D/YYYY format (e.g. 1/2/2007 -> Jan 2, 2007)
    raw_df["date"] = pd.to_datetime(raw_df["date"], format="%m/%d/%Y", errors="coerce")
    
    # 5. Clean Numeric and Float Columns
    float_cols = [
        "twelve_month_low",
        "twelve_month_high",
        "day_low",
        "day_high",
        "day_price",
        "previous_price",
        "change",
        "adjust",
    ]
    for col in float_cols:
        if col in raw_df.columns:
            raw_df[col] = clean_numeric_series(raw_df[col])

    # 6. Clean Percentage Column
    raw_df["change_pct"] = clean_percentage_series(raw_df["change_pct"])

    # 7. Clean Volume (fill NaN with 0, cast to int64)
    raw_df["volume"] = clean_numeric_series(raw_df["volume"]).fillna(0).astype("int64")

    # 8. Sort Chronologically
    raw_df.sort_values(by=["date", "code"], inplace=True)
    raw_df.reset_index(drop=True, inplace=True)

    # 9. Save Clean Dataset
    output_csv = os.path.join(data_dir, "cleaned_nse_stocks_2007_2012.csv")
    raw_df.to_csv(output_csv, index=False)
    print(f"[ETL] Successfully cleaned {len(raw_df):,} records!")
    print(f"[ETL] Saved clean dataset to: {output_csv}\n")

    return raw_df


if __name__ == "__main__":
    df_clean = run_etl()
    
    print("=== DATA INTEGRITY & SUMMARY STATISTICS ===")
    print(f"Date Range: {df_clean['date'].min().strftime('%Y-%m-%d')} to {df_clean['date'].max().strftime('%Y-%m-%d')}")
    print(f"Total Unique Tickers: {df_clean['code'].nunique()} companies")
    print(f"Total Trading Records: {len(df_clean):,}")
    print("\nData Types:")
    print(df_clean.dtypes)
    print("\nFirst 3 Cleaned Records:")
    print(df_clean[["date", "code", "name", "day_price", "volume", "change_pct"]].head(3))
