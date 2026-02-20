# take the cleaned Player_log.csv s and convert them into relational database tables
# develop locally then deploy to gcp once functional
# Use cloud functions ??

import pandas as pd
import duckdb
import json

# Connect to persistent DuckDB database
conn = duckdb.connect(database='mtga_local.duckdb')

# read each csv 1 game at a time and partition into the correct tables


csv_path = "/home/r3gal/develop/mtga_pipeline/data/parsed_csv/Filtered_Player_20260212_233143_test.csv"


# For each game
# save the deck_list in decks
# save the mulligans
# save the match details in matches
#   card draws -> draw_order

# add a new player to players if no match was found for player_id

# The csv will contain at min 1 game, can contain more
def load_csv_to_sql (csv_path):
    dtype_map = {
        'game_num': 'Int64',
        'timestamp': 'str',
        'event': 'str',
        'payload': 'str'
    }
    df = pd.read_csv(csv_path, dtype=dtype_map)
    df['payload'] = df['payload'].apply(json.loads)

    # group by game_num
    for game_num, group_df in df.groupby("game_num"):
        pass
    

    pass


# returns the next_deck_id, incremented
row = conn.execute("SELECT COALESCE(MAX(deck_id), 0) FROM decks").fetchone()
next_deck_id = row[0] + 1
def insert_deck (conn, df, player_id, match_id, next_deck_id):
    deck_obj = df.iloc[0]['payload'].get('request')
    nested = json.loads(deck_obj)

    # Gets the array of the main deck: cardId and quantity are keys
    # [{'cardId': 95192, 'quantity': 9},
    #  {'cardId': 93715, 'quantity': 1},
    #  {'cardId': 95200, 'quantity': 6}]

    deck_name = nested.get('Summary').get('Name')
    deck_list = nested.get('Deck').get('MainDeck')
    deck_sideboard = nested.get('Deck').get('Sideboard')
    deck_commander = nested.get('Deck').get('CommandZone')

    # convert to string for insertion
    deck_list_json = json.dumps(deck_list) if deck_list else None
    deck_sideboard_json = json.dumps(deck_sideboard) if deck_sideboard else None
    deck_commander_json = json.dumps(deck_commander) if deck_commander else None

    conn.execute(
        "INSERT INTO decks (deck_id, player_id, match_id, deck_list, deck_sideboard, deck_commander) VALUES (?, ?, ?, ?, ?, ?)",
        (next_deck_id, player_id, match_id, deck_list, deck_sideboard, deck_commander)
        )
    return next_deck_id + 1

# attempts to insert player, will skip if player_id is non_unique
def insert_player (conn, player_id, display_name, region):
    conn.execute(
        """
        INSERT INTO players (player_id, display_name, region)
        SELECT ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM players WHERE player_id = ?
        )
        """,
        (str(player_id), str(display_name), str(region), str(player_id))
    )