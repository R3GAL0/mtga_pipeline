## Objective

This project transforms Magic: The Gathering Arena (MTGA) client log files into a structured format and visual dashboard, enabling players to view statistics such as Win/Loss rates and card performance.

The ETL Pipeline Diagram is visualized [here](mtga_pipeline_flow.drawio.png).

The silver-layer schema is visualized in [ERD](erd_silver.png). (note: outdated, column names and types are not accurate)


## Process walkthrough

Following the [Pipeline Diagram](mtga_pipeline_flow.drawio.png). 

### Log Capture
- A player plays MTGA, generating a log file. 
- Logs are captured via the shell script [capture_data.sh](/mtga_pipeline/local/capture_data.sh). 

### Parsing
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


## Optional addons

- Track card draw order to matches table.
- Add opponent details (ie mana colors, deck archetype).
- Support for BO3 matches and other game modes.
- Track rank progression over time.


## Transformation/Ingestion Logic

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