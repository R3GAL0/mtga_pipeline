## Objective

This project transforms Magic: The Gathering Arena (MTGA) client log files into a structured format and visual dashboard, enabling players to view statistics such as Win/Loss rates and card performance.

The ETL Pipeline Diagram is visualized [here](mtga_pipeline_flow.png).

The silver-layer schema is defined locally for [DuckDB](/mtga_pipeline/cloud/silver_duckdb_schema.sql) and in the cloud for [BigQuery](/mtga_pipeline/cloud/silver_schema_bigquery.sql). It is visualized here [ERD](erd_silver.png) (note: the visualization is outdated, column names and types are not accurate)

The silver layer is fully normalized and designed to represent atomic game events.
The future gold layer will denormalize data into analytics-ready tables for dashboard consumption.

## Why This Project Is Interesting

- Parses semi-structured game client logs into structured relational models
- Handles deeply nested JSON payloads from live game events
- Pulls data from APIs for reference analysis
- Migrates from local DuckDB development to production BigQuery
- Demonstrates layered data architecture (Bronze → Silver → Gold)

## Process walkthrough

Following the [Pipeline Diagram](mtga_pipeline_flow.drawio.png). 

### Log Capture
- A player plays MTGA, generating a log file. 
- Logs are captured via the shell script [capture_data.sh](/mtga_pipeline/local/capture_data.sh). 

### Parsing (Bronze Layer)
- Logs are parsed using [python_parser.py](/mtga_pipeline/local/python_parser.py).
- Game data payloads are extracted and saved as CSV files.

### ETL to Silver Layer
- Using [make_tables.py](/mtga_pipeline/cloud/make_tables.py), CSVs are loaded, transformed, and inserted into the [silver table schema](/mtga_pipeline/cloud/silver_duckdb_schema.sql).



## Architecture Overview

| Layer                | Technology/Process                         |
| -------------------- | ------------------------------------------ |
| **Ingestion**        | Shell script captures MTGA log files       |
| **Processing**       | Python + Pandas parses and transforms logs |
| **Storage (Silver)** | GCP (cloud), DuckDB (local/Codespaces)     |
| **Analysis**         | BigQuery or local queries on DuckDB        |



## Tech Stack

- Python 
- Pandas
- DuckDB
- Google Cloud Platform (GCP)
- BigQuery
- Docker
- Looker Studio

## Transformation / Ingestion Logic

### Game Detection

- Each Player.log may contain multiple games and other client activity.
- **Game Start:** Detected via regex:

```
match_start_pattern = re.compile(
    r'^\[UnityCrossThreadLogger\]STATE CHANGED '
    r'\{"old":"ConnectedToMatchDoor_(?:ConnectedToGRE_Waiting|ConnectingToGRE)","new":"Playing"\}'
)
```
- **Game End:** Detected via string:
```
[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}
```

### Other Transformations

- **Extract Zones:** Parse zone information from each player's hand and discard
- **Map instanceId -> grpId:** Resolve card instances to unique card identifiers
- **Card Dimension Table:** Match unique card identifiers to card data, for lookup
- **Handle Partial Logs:** Ensure incomplete sessions are logged without breaking the pipeline

## Development Stages

How the pipeline was built step-by-step ([Pipeline Diagram](mtga_pipeline_flow.drawio.png)). 

### Completed

- capture_data.sh: Make a quick shell script to capture game data while the rest of the pipeline is built
- python_parser.py: Handles the initial parsing of the Player.log. Captures only game data
- silver_duckdb_schema.sql: Will do initial sql development locally then move to cloud afterwards
- make_tables_duckdb.py: Build out the logic to split the payloads from the response into RLDB flat tables
- GCP Bucket: Setup the GCP bucket and get familiar with the platform
- Cloud Run Jobs: Using the local version of the database deployed it to cloud run jobs as a Docker image.
- BigQuery: Setup BigQuery on GCP, enforce the schema

### To Be Completed / In Progress

- make_table_bigquery.sql: Refactor make_table_duckdb.sql to work with BigQuery on GCP, moving from local development to the cloud.  
- Gold layer transform: Perform aggregations on the silver layer tables to put on the dashbaord
- Looker Studio: Make the dashboard
- Extra Enhancements: Any extra enhancements (ie. draw order to matches tables)

## Future Enhancements

- Track card draw order to matches table.
- Add opponent details (ie mana colors, deck archetype).
- Support for BO3 matches and other game modes.
- Track rank progression over time.

## Extras / Learnings

- Motivation: Track my own MTGA stats and build a personal dashboard.

- Engineering Challenges Solved:

    - Handling nested JSON payloads
    - Deduplicating incomplete logs
    - Mapping instanceId → grpId

- Key Technical Decisions:
    - DuckDB for local querying
    - Pandas for parsing and processing
    - ETL separation into silver tables for structured data
- Lessons Learned: Regex parsing, list comprehension, log handling, structured data ingestion, building reproducible ETL pipelines, GCP, DuckDB