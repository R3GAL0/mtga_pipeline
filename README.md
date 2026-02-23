## Objective

Transform the output log files from the MTGA client games into a dashboard, so the user may view their statistics (Win/Loss, and the performance of specific cards)

The ETL Pipeline Diagram is visualized [here](mtga_pipeline_flow.drawio.png)

## Database Schema

The silver-layer schema is visualized in [ERD](erd_silver.png). (outdated)

## Process Flow / Execution Order

## Optional addons

- Add detils about the opponent to the matches table (ie mana colors)
- Add rank progression data

## Architecture Overview

- Ingestion: MTGA log files
- Processing: Python + Pandas
- Storage: PostgreSQL (Silver layer)
- Visualization: Streamlit dashboard
- Orchestration: Manual / Airflow

## Tech Stack

- Python 
- Pandas
- DuckDB
- GCP
- BigQuery

## Setup

1. Clone the repo
2. Create virtual environment
3. Install dependencies
4. Configure .env
5. Initialize database

## Transformation/Ingestion Logic

- How you detect game start
- How you extract zones
- How you map instanceId → grpId
- Deduplication logic
- Handling partial logs


## Extras

- Why I built this
- What engineering challenges I solved
- Key technical decisions
- What I learned