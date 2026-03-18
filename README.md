## Objective

This project transforms Magic: The Gathering Arena (MTGA) client log files into structured, analytics-ready data and a visual [dashboard](https://lookerstudio.google.com/s/kQwxZ_kW-hs), enabling players to evaluate win rates, deck performance, and opening hand strength for better mulligan decisions.

The ETL Pipeline is visualized [here](mtga_pipeline_flow.png).

The silver-layer schema is defined locally for [DuckDB](/mtga_pipeline/cloud/silver_duckdb_schema.sql) and in the cloud for [BigQuery](/mtga_pipeline/cloud/silver_schema_bigquery.sql), with an accompanying [ERD](erd_silver.png) (note: the visualization is outdated). The cloud pipeline is deployed with Docker containers.

The gold layer is built using dbt, with models available [here](/mtga_pipeline/cloud/gold_dbt/models/) and a schema reference [here](/mtga_pipeline/cloud/gold_dbt/gold_schema_bigquery.sql).

- The silver layer is fully normalized and represents atomic game events.
- The gold layer denormalizes data into analytics-ready tables for dashboard consumption.


### Why This Project Is Interesting

- Parses semi-structured game client logs into structured relational models
- Handles deeply nested JSON payloads from live game events
- Pulls data from APIs for reference analysis
- Migrates from local DuckDB development to production BigQuery
- Demonstrates layered data architecture (Bronze → Silver → Gold)

## Mulligans in Magic the Gathering

Magic uses the London Mulligan system.

At the start of each game, players draw 7 cards. They may choose to keep or mulligan their hand. If a player mulligans, they shuffle their hand back into their deck and draw a new 7 cards. After deciding to keep, they must place a number of cards equal to their mulligans on the bottom of their deck.

For example:

- 1 mulligan → keep 7, then bottom 1 card (play with 6)
- 2 mulligans → keep 7, then bottom 2 cards (play with 5)

While this system improves hand consistency, mulligans still carry a significant cost. In practice, even a single mulligan can reduce win rate by ~20%, with additional mulligans compounding the disadvantage.

This is largely explained by card advantage—starting with fewer cards reduces the number of options available to a player, limiting flexibility and decision-making.

### Why it matters

A key goal for players is to minimize mulligans while still avoiding weak opening hands. This creates a trade-off:

- Keep a suboptimal hand → risk poor performance
- Mulligan → guaranteed resource disadvantage

The metrics in this project aim to quantify that trade-off by:
- Measuring win rates by mulligan count
- Evaluating opening hand strength
- Identifying which cards contribute most to keepable hands

These insights help players:
- Make more informed mulligan decisions
- Optimize deck composition
- Replace consistently underperforming cards


## Tech Stack

- Python 
- Pandas
- DuckDB
- Google Cloud Platform (GCP)
- BigQuery
- Docker
- Data Build Tool (dbt)
- Looker Studio

## Architecture Overview

| Layer                | Technology/Process                         |
| -------------------- | ------------------------------------------ |
| **Ingestion**        | Shell script captures MTGA log files       |
| **Processing**       | Python + Pandas parses and transforms logs |
| **Landing Storage**  | GCP Bucket (cloud)                         |
| **ETL to Silver**    | Docker image deployed on GCP  / Locally with DuckDB  |
| **ETL to Gold**      | dbt aggregations on GCP into analyitics ready tables        |
| **Analysis**         | Looker Studio dashboard        |



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
    - These tables are then ingested by the Looker Studio [dashboard](https://lookerstudio.google.com/s/kQwxZ_kW-hs)


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
 
## Metrics

### Player Level
All player level metrics include all the player games and decks on record

Win Rates:
- win_rate_pct: The win rate (number of wins / total number of games).
- win_rate_30d_pct: The win rate for the last 30 days.
- win_rate_play: The win rate when the player goes first (is on the play).
- win_rate_draw: The win rate when the player goes second (is on the draw).
- first_50_wr: The win rate for the first 50 games recorded for the player.
- last_50_wr: The win rate for the last 50 games recorded for the player.
- wr_improvement: The improved win rate of the player over time (last_50_wr - first_50_wr).

Other:
- avg_mulligans: The average number of mulligans taken by the player per game
- avg_duration_sec: The average match duration, over all games and decks.

### Deck Level

Win Rates:
- win_rate_pct: The win rate (number of wins / total number of games).
- win_rate_play: The win rate when the player goes first (is on the play).
- win_rate_draw: The win rate when the player goes second (is on the draw).
- opener_win_rate: The win rate for each card in the deck when it is in the opening hand.

Power Score (Bayseian smoothing, k = 50):

    power_score = adjusted_wr * log(10+games)
    adjusted_wr = (wins+k*global_wr)/(games+k)

    where k = confidence weight (~50-100 games)
- total_power_score: Power score of the deck, using the player win rate as the global_wr
- play_power_score: Power score of the deck during 'on play' games (player goes first), using the deck win rate as the global_wr
- draw_power_score: Power score of the deck during 'on draw' games (player goes second), using the deck win rate as the global_wr
- card_oh_power_score: Power score of each card in the deck, using the deck win rate as the global_wr. (Only for opening hand win rates)

Other:
- avg_mulligans: The average number of mulligans taken by the player per game
- avg_duration_sec: The average match duration, over all games and decks.

### Card Level

To be added later
 

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