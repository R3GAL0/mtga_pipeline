## Objective

This project transforms Magic: The Gathering Arena (MTGA) client log files into a structured format and visual [dashboard](https://lookerstudio.google.com/s/kQwxZ_kW-hs), enabling players to view statistics such as Win/Loss rates and card performance on a per deck level.

The ETL Pipeline Diagram is visualized [here](mtga_pipeline_flow.png).

The silver-layer schema is defined locally for [DuckDB](/mtga_pipeline/cloud/silver_duckdb_schema.sql) and in the cloud for [BigQuery](/mtga_pipeline/cloud/silver_schema_bigquery.sql). It is visualized here [ERD](erd_silver.png) (note: the visualization is outdated, column names and types are not accurate). The cloud version is deployed with Docker containers.

The gold-layer tables are made with Data Build Tool (dbt), found [here](/mtga_pipeline/cloud/gold_dbt/models/). A rough version of the schema is [here](/mtga_pipeline/cloud/gold_dbt/gold_schema_bigquery.sql)

The silver layer is fully normalized and designed to represent atomic game events.
The gold layer denormalizes the data into analytics-ready tables for dashboard consumption.

## Why This Project Is Interesting

- Parses semi-structured game client logs into structured relational models
- Handles deeply nested JSON payloads from live game events
- Pulls data from APIs for reference analysis
- Migrates from local DuckDB development to production BigQuery
- Demonstrates layered data architecture (Bronze → Silver → Gold)

## Process walkthrough

Following the [Pipeline Diagram](mtga_pipeline_flow.png). 

### Log Capture
- A player plays MTGA, generating a log file. 
- Logs are captured via the shell script [capture_data.sh](/mtga_pipeline/local/capture_data.sh). 

### Parsing (Bronze Layer)
- Logs are parsed using [python_parser.py](/mtga_pipeline/local/python_parser.py).
- Game data payloads are extracted and saved as CSV files.

### ETL to Silver Layer (Local)
- Using [make_tables.py](/mtga_pipeline/cloud/make_tables.py), CSVs are loaded, transformed, and inserted into the [silver table schema](/mtga_pipeline/cloud/silver_duckdb_schema.sql).


### ETL to Silver Layer (Cloud)
- CSVs are loaded into a GCP bucket
- A Docker container is executed [here](/mtga_pipeline/cloud/silver_bigquery/). (Can be schedueled on an upload event.)
    - It extracts the list of CSVs to process
    - Moves those CSVs to /tmp/data for processing
    - Transforms/inserts the data into BigQuery tables
    - Updates the process list
    - Performs a cleanup on the memory

### Gold Layer Transform (Cloud)
- A dbt process is executed [here](/mtga_pipeline/cloud/gold_dbt/). (Can be schedueled on a weekly/daily/hourly basis.)
    - Using the silver layer tables it aggregates the results into materialized tables
    - These tables are then ingested by the [dashboard](https://lookerstudio.google.com/s/kQwxZ_kW-hs)

## Architecture Overview

| Layer                | Technology/Process                         |
| -------------------- | ------------------------------------------ |
| **Ingestion**        | Shell script captures MTGA log files       |
| **Processing**       | Python + Pandas parses and transforms logs |
| **Landing Storage**  | GCP Bucket (cloud)                         |
| **ETL to Silver**    | Docker image deployed on GCP  / Locally with DuckDB  |
| **ETL to Gold**      | dbt aggregations on GCP into analyitics ready tables        |
| **Analysis**         | Looker Studio dashboard        |



## Tech Stack

- Python 
- Pandas
- DuckDB
- Google Cloud Platform (GCP)
- BigQuery
- Docker
- Data Build Tool (dbt)
- Looker Studio

## Transformation / Ingestion Logic

### Parser (Bronze Layer)

- Separate each game into distinct sets, and save only the game data
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

- **Handle Partial Logs:** Ensure incomplete sessions are logged without breaking the pipeline

### Silver Layer Transforms

- **PK Tracking:** Handle primary key uniqueness and incrementing (BigQuery does not handle natively)
- **Extract Zones:** Parse zone information from each player's hand and discard
- **Map instanceId -> grpId:** Resolve card instances to unique card identifiers
- **Card Dimension Table:** Match unique card identifiers to card data, for lookup

### Gold Layer Transforms

- **Mulligan Counts:** Pivoted turn 1 hand data to calculate counts and win rates for 0-7 mulligans, along with the average mulligans per player.
- **Window Functions:** Used rolling window calculations over match timestamps to compute 30-day match counts and rolling win rates per player.
- **Most Played Deck:** Aggregated matches by deck per player and selected the deck with the highest match count using ROW_NUMBER().
- **Aggregations:** Computed total matches, wins, losses, average match duration, and mulligan win rates for each player.
 

 

## Development Stages

How the pipeline was built step-by-step ([Pipeline Diagram](mtga_pipeline_flow.png)). 


- capture_data.sh: Make a quick shell script to capture game data while the rest of the pipeline is built
- python_parser.py: Handles the initial parsing of the Player.log. Captures only game data
- silver_duckdb_schema.sql: Will do initial sql development locally then move to cloud afterwards
- make_tables_duckdb.py: Build out the logic to split the payloads from the response into RLDB flat tables
- GCP Bucket: Setup the GCP bucket and get familiar with the platform
- Cloud Run Jobs: Using the local version of the database deployed it to cloud run jobs as a Docker image.
- BigQuery: Setup BigQuery on GCP, enforce the schema
- make_table_bigquery.sql: Refactor make_table_duckdb.sql to work with BigQuery on GCP, moving from local development to the cloud.  
- Gold layer transform: Perform aggregations on the silver layer tables to put on the dashbaord
- Looker Studio: Make the dashboard


## Future Enhancements

- Track card draw order to matches table.
    - Add additional metrics for card impact based ondraw timing.
- Add opponent details (ie mana colors, deck archetype).
- Support for BO3 matches and other game modes.
- Track rank progression over time.
- Scheduel the pipeline to run on events/timeline for automation

## Extras / Learnings

- Motivation: Track my own MTGA stats and build a personal dashboard.

- Engineering Challenges Solved:

    - Handling nested JSON payloads
    - Deduplicating incomplete logs
    - Mapping instanceId → grpId
    - Cloud deployment and resource monitoring
    - Model design

- Key Technical Decisions:
    - DuckDB for local querying
    - Pandas for parsing and processing
    - ETL separation into silver tables for structured data
    - GCP, Docker, dbt tech stack
- Lessons Learned: Regex parsing, list comprehension, log handling, structured data ingestion, building reproducible ETL pipelines, GCP, DuckDB, Docker, dbt, Looker