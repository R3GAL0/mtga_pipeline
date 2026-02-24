-- DuckDB schema
-- For testing locally before deployment to GCP with BigQuery
-- DuckDB schema
-- For testing locally before deployment to GCP with BigQuery

-- Keys will be incremented using python

DROP TABLE IF EXISTS turn1_hands;
DROP TABLE IF EXISTS decks;
DROP TABLE IF EXISTS matches;
DROP TABLE IF EXISTS dim_cards;
DROP TABLE IF EXISTS players;

CREATE TABLE players (
    player_id VARCHAR PRIMARY KEY,
    display_name VARCHAR
    -- region VARCHAR
);

CREATE TABLE dim_cards (
    arena_id VARCHAR PRIMARY KEY,
    oracle_id VARCHAR,
    name VARCHAR,
    released_at DATE,
    scryfall_uri VARCHAR,
    mana_cost VARCHAR,
    cmc INTEGER,
    colors VARCHAR,
    color_identity VARCHAR,
    type_line VARCHAR,
    set_code VARCHAR,
    set_name VARCHAR,
    set_type VARCHAR,
    rarity VARCHAR,
    legalities VARCHAR
);

CREATE TABLE matches (
    match_id BIGINT PRIMARY KEY,
    deck_id BIGINT NOT NULL,
    player_id VARCHAR,
    player_seat INTEGER,            -- 1 or 2
    start_time TIMESTAMP NOT NULL,
    duration INTEGER,               -- SECONDS
    winner_seat VARCHAR,            -- 1 or 2
    game_format VARCHAR,
    draw_order VARCHAR              -- to be implemented
);

CREATE TABLE decks (
    deck_id  BIGINT PRIMARY KEY,
    player_id VARCHAR NOT NULL,
    match_id BIGINT NOT NULL,
    deck_name VARCHAR,
    deck_list VARCHAR,
    deck_sideboard VARCHAR,
    deck_commander VARCHAR
);

CREATE TABLE turn1_hands (
    hand_id BIGINT PRIMARY KEY,
    player_id VARCHAR NOT NULL,
    match_id BIGINT NOT NULL,
    initial_hand VARCHAR NOT NULL,
    mulliganCount INTEGER,
    final_hand VARCHAR,
    went_first BOOLEAN
);

