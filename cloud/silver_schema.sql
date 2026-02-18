-- Layer: Silver
-- Target: BigQuery
-- Description:
-- Normalized schema built from MTGA Player.log files
-- Designed for analytical workloads (winrate, deck and mulligan analysis, rank tracking)
-- Tables are modeled at event-level grain for downstream aggregation.

-- Foreign keys are documented but not enforced (BigQuery limitation).
-- Referential integrity is handled in upstream transformations.

CREATE TABLE `mtgapipeline.mtga_silver.players` (
  player_id STRING NOT NULL,       -- PK, from Wizards of the Coast
  display_name STRING,             -- Display name of the player
  region STRING                    -- Region the player connects from
);

CREATE TABLE `mtgapipeline.mtga_silver.cards` (
  card_id INT64 NOT NULL,          -- PK
  card_name STRING,                -- Name of the Card
  card_type STRING,                -- Type on the Card (Land, Creature, Sorcery, Instant, etc)
  mana_cost STRING,                -- Mana cost of the card (ie 2BB for 2 colorless 2 black mana)
  card_set STRING,                 -- What set the card was released in
  legal_formats STRING,            -- What formats the card is legal in
  card_color STRING                -- What are the colors on the card (ie UWBGR, C = colorless)
);

CREATE TABLE `mtgapipeline.mtga_silver.matches` (
  match_id INT64 NOT NULL,          -- PK
  start_time TIMESTAMP NOT NULL,    -- Start of match
  duration INT64,                   -- Match duration in seconds
  winner_id STRING NOT NULL,        -- References players.player_id
  loser_id STRING NOT NULL,         -- References players.player_id
  first_player_id STRING,           -- References players.player_id
  format STRING                     -- Match format (standard, historic, brawl, etc)
)
PARTITION BY DATE(start_time)
CLUSTER BY winner_id, loser_id;

CREATE TABLE `mtgapipeline.mtga_silver.decks` (
  deck_id INT64 NOT NULL,           -- PK
  player_id STRING NOT NULL,        -- FK, References players.player_id
  match_id INT64 NOT NULL,          -- FK, References matches.match_id
  legal_formats STRING              -- What formats the deck is legal in
)
CLUSTER BY player_id;

CREATE TABLE `mtgapipeline.mtga_silver.deck_cards` (
  deck_id INT64 NOT NULL,           -- PK, References decks.deck_id
  card_id INT64 NOT NULL,           -- PK, References cards.card_id
  card_quantity INT64               -- Number of this card in the deck (1-4)
)
CLUSTER BY deck_id;

CREATE TABLE `mtgapipeline.mtga_silver.turn1_hands` (
  match_id INT64 NOT NULL,           -- PK, References matches.match_id
  player_id STRING NOT NULL,         -- PK, References players.player_id
  initial_hand ARRAY<STRING>,        -- Cards in hand prior to selecting which to keep (if mulligan > 0)
  mulligans INT64,                   -- Current mulligan number for the hand (0-7)
  final_hand ARRAY<STRING>,          -- Hand kept, is size = 7 - mulligans
  went_first BOOL                    -- If the player with this hand went first
)
CLUSTER BY match_id, player_id;

CREATE TABLE `mtgapipeline.mtga_silver.rank_progression` (
  player_id STRING NOT NULL,         -- PK, References players.player_id
  rank_dt TIMESTAMP NOT NULL,        -- PK, timestamp when this rank was achieved
  rank_tier STRING,                  -- The tier of the rank (Gold 4, Plat 1, etc)
  rank_sub_tier STRING               -- The number of pips within that tier (0-6)
)
PARTITION BY DATE(rank_dt)
CLUSTER BY player_id;
