-- Layer: Gold
-- Target: BigQuery
-- Description:


-- bq query --use_legacy_sql=false < gold_schema_bigquery.sql


DROP TABLE IF EXISTS mtgapipeline.mtga_gold.player_metrics;
DROP TABLE IF EXISTS mtgapipeline.mtga_gold.deck_metrics;
DROP TABLE IF EXISTS mtgapipeline.mtga_gold.card_metrics;

CREATE TABLE `mtgapipeline.mtga_gold.player_metrics` (
  player_id             STRING NOT NULL,-- PK, from Wizards of the Coast
  display_name          STRING,         -- Display name of the player

  total_matches         INT64,          -- Total matches played
  total_wins            INT64,          -- Total wins
  total_losses          INT64,          -- Total losses
  win_rate              NUMERIC,        -- total_wins / total_matches
  win_rate_30d          NUMERIC,        -- total_wins / total_matches, over the last 30 days
  avg_duration_sec      NUMERIC,        -- Average match duration
  
  avg_mulligans         NUMERIC,        -- Average mulligans per match
  mulligan0_win_rate    NUMERIC,        -- Win rate with 0 mulligans
  mulligan1_win_rate    NUMERIC,        -- Win rate with 1 mulligan
  mulligan2_win_rate    NUMERIC,        -- Win rate with 2 mulligans
  most_played_deck_id   INT64,          -- Deck_id of most played deck
  most_played_deck_name STRING,         -- Name of the most played deck
  time_last_played      TIMESTAMP       -- Timestamp of most recent match
  
);

-- also check deck performace vs different color matchups? (need to extract opponent colors)
CREATE TABLE `mtgapipeline.mtga_gold.deck_metrics` (
  deck_id               INT64 NOT NULL, -- PK
  deck_name             STRING,         -- player defined deck name
  player_id             STRING,         -- From Wizards of the Coast
  display_name          STRING,         -- Display name of the player

  avg_duration_sec      NUMERIC,        -- Average match duration
  total_matches         INT64,          -- Total matches played, with this deck
  total_wins            INT64,          -- Total wins, with this deck
  win_rate              NUMERIC,        -- total_wins / total_matches
  avg_mulligans         NUMERIC,        -- Average mulligans per match
  mulligan0_win_rate    NUMERIC,        -- Win rate with 0 mulligans
  mulligan1_win_rate    NUMERIC,        -- Win rate with 1 mulligan
  mulligan2_win_rate    NUMERIC,        -- Win rate with 2 mulligans
  colors                STRING,         -- Colors in the deck (GU, WRU)
  cmc_curve             ARRAY<INT64>,   -- number of cards per cost
  cmc_avg               NUMERIC,        -- avgerage mana cost of all cards in the deck 
  matchup_winrate       ARRAY<STRUCT<   -- Winrate per matchup (not implemented)
                            opp_colors STRING,
                            total_matches INT64,
                            total_wins INT64,
                            total_losses INT64,
                            win_rate NUMERIC
                        >>
)
CLUSTER BY player_id;


-- card stats from when the card is in your opening hand
CREATE TABLE `mtgapipeline.mtga_gold.card_metrics` (
  player_id             STRING NOT NULL,-- Player identifier
  deck_id               INT64 NOT NULL, -- Deck identifier
  arena_id              INT64 NOT NULL, -- Card identifier
  card_name             STRING,         -- Name of the card
  win_rate_draw         NUMERIC,        -- total_wins / total_matches, when the card is drawn (anytime during the match)
                                        -- Not currently implemented
  win_rate_opener       NUMERIC,        -- total_wins / total_matches, when the card is in the opening hand
  total_in_deck         INT64,          -- total_wins / total_matches, when the card is in the opening hand
  draw_chance           NUMERIC        -- deck size / total_in_deck, chance the card will be drawn 
)
CLUSTER BY player_id, deck_id;
