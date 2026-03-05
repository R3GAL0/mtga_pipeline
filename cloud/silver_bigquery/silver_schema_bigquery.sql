-- Layer: Silver
-- Target: BigQuery
-- Description:
-- Normalized schema built from MTGA Player.log files
-- Designed for analytical workloads (winrate, deck and mulligan analysis, rank tracking)
-- Tables are modeled at event-level grain for downstream aggregation.

-- Foreign keys are documented but not enforced (BigQuery limitation).
-- Referential integrity is handled in upstream transformations.

-- bq query --use_legacy_sql=false < silver_schema_bigquery.sql

DROP TABLE IF EXISTS mtgapipeline.mtga_silver.players;
DROP TABLE IF EXISTS mtgapipeline.mtga_silver.dim_cards;
DROP TABLE IF EXISTS mtgapipeline.mtga_silver.matches;
DROP TABLE IF EXISTS mtgapipeline.mtga_silver.decks;
DROP TABLE IF EXISTS mtgapipeline.mtga_silver.turn1_hands;
-- DROP TABLE IF EXISTS mtgapipeline.mtga_silver.pk_counter;

CREATE TABLE `mtgapipeline.mtga_silver.players` (
  player_id STRING NOT NULL,       -- PK, from Wizards of the Coast
  display_name STRING              -- Display name of the player
);

CREATE TABLE `mtgapipeline.mtga_silver.dim_cards` (
    arena_id INT64 NOT NULL,      -- PK, MTGA key
    oracle_id STRING NOT NULL,    -- PK, Scryfall key
    card_name STRING,             -- Name of the Card
    scryfall_uri STRING,          -- Link to the card on scryfall (shows additional card data)
    mana_cost STRING,             -- Mana cost of the card, type dependent
    cmc INT64,                    -- (combined mana cost) total mana cost of the card, type agnostic
    colors STRING,                -- Unique mana cost colors (empty for costless cards, ie lands)
    color_identity STRING,        -- Unique color associations (populated for costless cards, ie lands)
    type_line STRING,             -- Card types (main type — sub type — sub sub type, )
    set_code STRING,              -- Code of the set the card is from
    set_name STRING,              -- Name of the set the card is from
    set_type STRING,              -- Type of the set the card is from
    rarity STRING                 -- Rarity of the Card (Common, Uncommon, rare, mythic rare)
);

CREATE TABLE `mtgapipeline.mtga_silver.matches` (
  match_id INT64 NOT NULL,        -- PK
  deck_id INT64 NOT NULL,         -- FK, references decks
  player_id STRING,               -- FK, refrences players
  player_seat INT64,              -- seat of the player_id player (1 or 2)
  start_time TIMESTAMP NOT NULL,  -- Start of match
  duration_sec INT64,             -- Match duration in seconds
  winner_seat INT64,              -- which seat won the match (1 or 2)
  game_format STRING,             -- Match format (standard, event, historic, brawl, etc)
  draw_order ARRAY<STRUCT<cards ARRAY<INT64>>> -- The order of cards drawn during the match after the starting hand.
                                               -- Separated by turn drawn, using oracle_id to track card data
)
CLUSTER BY player_id, deck_id;
-- draw_order has format:
-- [
--   { "cards": [123, 456, 789] },  -- (turn 1)
--   { "cards": [111, 222] }        -- (turn 2)
-- ]

CREATE TABLE `mtgapipeline.mtga_silver.decks` (
  deck_id INT64 NOT NULL,           -- PK
  player_id STRING NOT NULL,        -- FK, References players.player_id
  match_id INT64 NOT NULL,          -- FK, References matches.match_id
  set_code STRING,                  -- The code of the most recent set when this deck was played
  deck_name STRING,                 -- player defined deck name
  deck_list ARRAY<INT64>,           -- The oracle_id of each card in the deck, repeated for duplicates
  deck_sideboard ARRAY<INT64>,      -- The oracle_id of cards in the sideboard. repeated for duplicates, LIMIT LENGTH=15)
                                             -- Only for 'Best Of 3' game modes, otherwise null.
  deck_commander STRING,             -- The comander for the deck. (Only used for game_format = 'commander')
  deck_hash STRING,                 -- A hashed version of the deck list for uniqueness checks
  side_hash STRING                  -- A hashed version of the sideboard for uniqueness checks
)
CLUSTER BY player_id;

CREATE TABLE `mtgapipeline.mtga_silver.turn1_hands` (
  hand_id INT64 NOT NULL,           -- PK
  player_id STRING NOT NULL,        -- FK, references players
  match_id INT64 NOT NULL,          -- FK, references matches
  initial_hand ARRAY<INT64>,        -- The hand drawn, using oracle_id, prior to selecting cards to keep if mulliganCount > 0
  mulliganCount INT64,              -- The mulligan count for the hand (0-7)
  discarded ARRAY<INT64>,          -- The hand after selecting cards to mulligan, using oracle_id
  went_first BOOL                   -- If the player went first
)
CLUSTER BY match_id, player_id;

-- stores the most recently used/highest PK for each
CREATE TABLE `mtgapipeline.mtga_silver.pk_counter` (
  pk_name STRING NOT NULL,          -- (match_id, deck_id, hand_id)
  current_value INT64 NOT null
);

-- CREATE TABLE `mtgapipeline.mtga_silver.rank_progression` (
--   player_id STRING NOT NULL,         -- PK, References players.player_id
--   rank_dt TIMESTAMP NOT NULL,        -- PK, timestamp when this rank was achieved
--   rank_tier STRING,                  -- The tier of the rank (Gold 4, Plat 1, etc)
--   rank_sub_tier STRING               -- The number of pips within that tier (0-6)
-- )
-- PARTITION BY DATE(rank_dt)
-- CLUSTER BY player_id;
