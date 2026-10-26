"""
ICS 2315 - Data Architecture & Warehousing
Assignment 2: Graph Data Analytics with Neo4j

This script handles:
1. Connecting to local Neo4j Docker container via the official Python neo4j driver.
2. Ingesting 'TRAFFIC ACCIDENTS DATA CLEANED.xlsx' into a Property Graph:
   - Core Nodes: (:Accident), (:Traffic), (:Location), (:Cause), (:County).
   - Relationship Ratings: Defining and rating relationships based on severity parameters
     (impact levels: 'Severe', 'Moderate', 'Minor' and numeric severity scores).
3. Graph Constraints & Indexes for fast traversal.
4. Executing Analytical Cypher Queries:
   - Query 1: Top 5 Dangerous Highways by Fatalities & Severe Incidents.
   - Query 2: Multi-Hop Root Cause Traversal along the high-risk Nakuru corridor.
   - Query 3: Blackspot Degree Centrality Analysis.
   - Query 4: Severity Parameter Impact Level Distribution.
5. Providing Interactive Visualization Cypher queries for Neo4j Browser (http://localhost:7474).
"""

import os
import sys
import pandas as pd
import numpy as np
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Ensure UTF-8 output encoding on Windows PowerShell
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# Load environment configuration
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "coursework2026")


def get_neo4j_driver():
    """Initializes and returns the Neo4j driver."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def compute_severity(total_dead: int, spot_freq: int, risk_level: str):
    """
    Computes relationship severity parameters as required by the assignment guidelines:
    'Define and rate relationships based on severity parameters
     (e.g., classifying impact levels such as severe).'
    """
    risk_weights = {"High": 3.0, "Medium": 2.0, "Low": 1.0}
    r_weight = risk_weights.get(str(risk_level).strip(), 1.0)

    # Weighted severity score calculation
    score = round((float(total_dead) * 2.5) + float(spot_freq) + r_weight, 2)

    if total_dead >= 3 or str(risk_level).strip() == "High" or score >= 8.0:
        impact = "Severe"
    elif total_dead >= 1 or str(risk_level).strip() == "Medium" or score >= 4.0:
        impact = "Moderate"
    else:
        impact = "Minor"

    return impact, score


def load_and_clean_data(filepath: str = None) -> pd.DataFrame:
    """Reads Excel file and standardizes text, dates, and severity fields."""
    if filepath is None:
        filepath = "data/TRAFFIC ACCIDENTS DATA CLEANED.xlsx" if os.path.exists("data/TRAFFIC ACCIDENTS DATA CLEANED.xlsx") else "TRAFFIC ACCIDENTS DATA CLEANED.xlsx"
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Excel dataset '{filepath}' not found.")

    print(f"[GRAPH ETL] Reading '{filepath}'...")
    df = pd.read_excel(filepath)

    # Standardize string fields
    string_cols = ["accident_spot", "area", "county", "road_highway", "accident_cause", "victims", "risk_level"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Clean inconsistent county names
    county_fix = {
        "Nairobi ": "Nairobi",
        "Muranga": "Murang'a",
        "Uansin Gishi": "Uasin Gishu",
    }
    df["county"] = df["county"].replace(county_fix)

    # Ensure numeric columns are properly typed
    df["total_dead"] = df["total_dead"].fillna(0).astype(int)
    df["accident_spot_freq"] = df["accident_spot_freq"].fillna(1).astype(int)
    df["road_highway_freq"] = df["road_highway_freq"].fillna(1).astype(int)
    df["accident_cause_deaths"] = df["accident_cause_deaths"].fillna(0.0).astype(float)

    # Parse dates to ISO string YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Generate unique Accident Identifiers (ACC_001, ACC_002, ...)
    df["accident_id"] = [f"ACC_{i+1:03d}" for i in range(len(df))]

    # Compute rated severity parameters
    severities = []
    scores = []
    for row in df.itertuples():
        impact, score = compute_severity(row.total_dead, row.accident_spot_freq, row.risk_level)
        severities.append(impact)
        scores.append(score)

    df["impact_level"] = severities
    df["severity_score"] = scores

    print(f"[GRAPH ETL] Processed {len(df)} records. Found {df['impact_level'].value_counts().to_dict()}.\n")
    return df


def setup_constraints(session):
    """Creates schema constraints for optimal graph lookups."""
    print("[GRAPH SCHEMA] Applying uniqueness constraints and indexes...")
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Accident) REQUIRE a.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Traffic) REQUIRE t.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE l.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Cause) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (co:County) REQUIRE co.name IS UNIQUE",
    ]
    for cql in constraints:
        session.run(cql)
    print("  * Constraints created for (:Accident), (:Traffic), (:Location), (:Cause), (:County).\n")


def build_graph(driver, df: pd.DataFrame):
    """
    Constructs the Property Graph in Neo4j:
    - Clears previous graph.
    - Creates Nodes and Relationships with rated severity properties.
    """
    with driver.session() as session:
        # Clear existing data in the graph
        session.run("MATCH (n) DETACH DELETE n")
        print("[GRAPH BUILD] Reset existing Neo4j graph database.")

        setup_constraints(session)

        cypher_merge = """
        UNWIND $rows AS row
        MERGE (a:Accident {id: row.accident_id})
        ON CREATE SET
            a.date = date(row.date),
            a.total_dead = row.total_dead,
            a.victims = row.victims,
            a.risk_level = row.risk_level,
            a.severity_score = row.severity_score

        MERGE (t:Traffic {name: row.road_highway})
        ON CREATE SET t.frequency = row.road_highway_freq

        MERGE (l:Location {name: row.accident_spot})
        ON CREATE SET l.area = row.area, l.frequency = row.accident_spot_freq

        MERGE (c:Cause {name: row.accident_cause})
        ON CREATE SET c.cause_deaths_weight = row.accident_cause_deaths

        MERGE (co:County {name: row.county})

        // Core rated relationship to Traffic
        MERGE (a)-[r1:OCCURRED_ON]->(t)
        SET r1.severity = row.impact_level,
            r1.severity_score = row.severity_score,
            r1.fatalities = row.total_dead

        // Topological structural relationships
        MERGE (a)-[r2:LOCATED_AT]->(l)
        MERGE (l)-[r3:PART_OF]->(t)
        MERGE (l)-[r4:IN_COUNTY]->(co)
        MERGE (a)-[r5:CAUSED_BY]->(c)
        SET r5.fatalities = row.total_dead
        """

        records_payload = df.to_dict(orient="records")
        session.run(cypher_merge, rows=records_payload)

        # Count total elements
        node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        print(f"[GRAPH BUILD] Successfully loaded Property Graph:")
        print(f"  * Nodes Created: {node_count:,}")
        print(f"  * Relationships Created: {rel_count:,}\n")


def run_graph_analytics(driver):
    """Executes key graph analytics queries answering assignment requirements."""
    print("=" * 75)
    print("[GRAPH ANALYTICS] CYPHER QUERY EXPERIMENTS (For Coursework Report)")
    print("=" * 75)

    with driver.session() as session:
        # Query 1: Most Dangerous Highways by Severe Incidents & Fatalities
        print("\n1. Top 5 Most Dangerous Highways (Fatalities & Severity):")
        q1 = """
        MATCH (a:Accident)-[r:OCCURRED_ON]->(t:Traffic)
        RETURN t.name AS highway,
               count(a) AS total_accidents,
               sum(r.fatalities) AS total_fatalities,
               count(CASE WHEN r.severity = 'Severe' THEN 1 END) AS severe_accidents,
               round(avg(r.severity_score), 2) AS avg_severity_score
        ORDER BY total_fatalities DESC, severe_accidents DESC
        LIMIT 5
        """
        for rank, row in enumerate(session.run(q1), 1):
            print(f"   {rank}. {row['highway']}: "
                  f"Fatalities = {row['total_fatalities']} | "
                  f"Total Incidents = {row['total_accidents']} "
                  f"({row['severe_accidents']} Severe) | "
                  f"Avg Score = {row['avg_severity_score']}")

        # Query 2: Multi-Hop Root Cause Traversal (Nakuru Corridor Driver Analysis)
        print("\n2. Multi-Hop Traversal: Primary Fatality Drivers in Nakuru County:")
        q2 = """
        MATCH (co:County {name: 'Nakuru'})<-[:IN_COUNTY]-(l:Location)<-[:LOCATED_AT]-(a:Accident)-[:CAUSED_BY]->(c:Cause)
        RETURN c.name AS cause,
               count(a) AS incident_count,
               sum(a.total_dead) AS total_fatalities,
               collect(DISTINCT l.name) AS blackspots
        ORDER BY total_fatalities DESC
        LIMIT 5
        """
        for rank, row in enumerate(session.run(q2), 1):
            spots_preview = ", ".join(row['blackspots'][:3])
            print(f"   {rank}. Cause: '{row['cause']}': "
                  f"{row['total_fatalities']} deaths across {row['incident_count']} accidents "
                  f"(Spots: {spots_preview})")

        # Query 3: Blackspot Degree Centrality
        print("\n3. High-Degree Blackspot Centrality (Top Incident Locations):")
        q3 = """
        MATCH (l:Location)<-[:LOCATED_AT]-(a:Accident)-[r:OCCURRED_ON]->(t:Traffic)
        RETURN l.name AS spot,
               l.area AS area,
               t.name AS highway,
               count(a) AS incidents,
               sum(r.fatalities) AS fatalities
        ORDER BY incidents DESC, fatalities DESC
        LIMIT 5
        """
        for rank, row in enumerate(session.run(q3), 1):
            print(f"   {rank}. [{row['spot']}] Area: {row['area']} | Road: {row['highway']} -> "
                  f"{row['incidents']} incidents ({row['fatalities']} deaths)")

        # Query 4: Severity Parameter Impact Level Distribution
        print("\n4. Rated Relationship Severity Distribution:")
        q4 = """
        MATCH ()-[r:OCCURRED_ON]->()
        RETURN r.severity AS severity,
               count(r) AS accident_count,
               sum(r.fatalities) AS total_fatalities,
               round(avg(r.severity_score), 2) AS avg_score
        ORDER BY total_fatalities DESC
        """
        for row in session.run(q4):
            print(f"   * [{row['severity']}] Impact Level: "
                  f"{row['accident_count']} links | {row['total_fatalities']} deaths | Avg Score = {row['avg_score']}")

    print("\n" + "=" * 75 + "\n")


def print_visualization_guide():
    """Prints instructions for interactive graph visualization in the Neo4j Browser."""
    print("=" * 75)
    print("[VISUALIZATION GUIDE] INTERACTIVE EXPLORATION IN NEO4J BROWSER")
    print("=" * 75)
    print("You can view and interact with the 3D/2D graph network in your web browser:")
    print("1. Open: http://localhost:7474")
    print("2. Login: Username 'neo4j' | Password 'coursework2026'")
    print("3. Paste and run this Cypher query to visualize high-severity accident clusters:")
    print("\n   MATCH (a:Accident)-[r:OCCURRED_ON {severity: 'Severe'}]->(t:Traffic)")
    print("   MATCH (a)-[:LOCATED_AT]->(l:Location)")
    print("   MATCH (a)-[:CAUSED_BY]->(c:Cause)")
    print("   RETURN a, r, t, l, c LIMIT 50;\n")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    driver = get_neo4j_driver()
    try:
        # 1. Clean Data & Rate Severity
        df_clean = load_and_clean_data()

        # 2. Build Property Graph
        build_graph(driver, df_clean)

        # 3. Execute Cypher Analytics
        run_graph_analytics(driver)

        # 4. Display Browser UI Guide
        print_visualization_guide()
    finally:
        driver.close()
        print("[NEO4J] Connection closed.")
