# ICS 2315: Data Architecture & Warehousing Coursework

**Author:** Kelvin Abidha  
**Registration Number:** SCT212-0063/2023  
**Coursework Deliverable:** NoSQL Data Processing & Graph Data Analytics  
**Reference Textbook:** *Database System Concepts (7th Edition)*, Chapter 10: Big Data

---

## 📌 Project Overview

- **Assignment 1: NoSQL Data Processing (10 Marks)**
  - **MongoDB**: Document schema modeling, single and compound indexing experiments (`COLLSCAN` vs `IXSCAN`), execution plan diagnostics, and aggregation pipelines.
  - **Apache Cassandra**: Distributed keyspace configuration, query-driven data modeling (Chebotko methodology), Murmur3 token ring partitioning, and scan rejection experiments.
  - **Dataset**: Nairobi Securities Exchange (NSE) Daily Trading Data (2007–2012, 81,504 records).

- **Assignment 2: Graph Data Analytics (10 Marks)**
  - **Neo4j**: Property Graph schema modeling, core entities (`:Accident`, `:Traffic`, `:Location`, `:Cause`, `:County`), rated severity parameter links (`Severe`, `Moderate`, `Minor`), and Cypher multi-hop traversals.
  - **Dataset**: Geocoded Road Traffic Accident records (375 fatalities).

- **Final Deliverable: Summary Report (10 Marks)**
  - Comprehensive report formatted in Markdown ([`ICS_2315_Final_Summary_Report.md`](ICS_2315_Final_Summary_Report.md)) and print-ready executive HTML ([`report.html`](report.html)).

---

## 📁 Repository Structure

```text
├── data/                                # Cleaned & raw datasets
│   ├── NSE_data_all_stocks_2007.csv     # Historical NSE datasets (2007-2012)
│   ├── ...
│   ├── NSE_data_all_stocks_2012.csv
│   ├── TRAFFIC ACCIDENTS DATA CLEANED.xlsx # Traffic accident records
│   └── cleaned_nse_stocks_2007_2012.csv # Unified ETL output (81,504 rows)
│
├── docs/                                # Documentation & screenshots
│   ├── figures/                         # Neo4j visualization screenshots
│   │   ├── figure1.png                  # Macro cluster network
│   │   ├── figure2.png                  # Edge severity property inspector
│   │   └── figure3.png                  # Nakuru corridor subgraph
│   └── guidelines/                      # Course guidelines & syllabus
│       ├── ICS 2315 Assignment Guidelines.md
│       ├── ICS 2315 Assignment Guidelines.pdf
│       └── WhatsApp Image 2026-10-08 at 09.40.54.jpeg
│
├── src/                                 # Implementation source code
│   ├── etl.py                           # Data extraction & cleaning pipeline
│   ├── mongo_manager.py                 # MongoDB loader, indexing benchmark, aggregations
│   ├── cassandra_manager.py             # Cassandra schema, ingestion, query demos
│   └── graph_manager.py                 # Neo4j property graph & Cypher analytics
│
├── docker-compose.yml                   # Cassandra & Neo4j orchestration
├── requirements.txt                     # Pinned Python dependencies
├── .env.example                         # Environment configuration template
├── .gitignore                           # Git ignore rules
├── ICS_2315_Final_Summary_Report.md     # Academic summary report
├── report.html                          # Executive visual report (Print to PDF)
└── report.pdf                           # Final exported PDF deliverable
```

---

## 🚀 Quickstart & Reproduction

### 1. Environment Setup
```bash
# Clone the repository
git clone https://github.com/KelvinAbidha/Data-Warehousing-Project.git
cd "Data-Warehousing-Project"

# Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Infrastructure Setup
```bash
# Start lightweight Cassandra (port 9042) and Neo4j (ports 7474, 7687)
docker compose up -d

# Copy environment configuration
cp .env.example .env
```

### 3. Run Pipelines
```bash
# Run ETL Pipeline
python src/etl.py

# Run MongoDB Ingestion & Benchmarking
python src/mongo_manager.py

# Run Cassandra Query-Driven Modeling
python src/cassandra_manager.py

# Run Neo4j Graph Analytics
python src/graph_manager.py
```
