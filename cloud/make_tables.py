# take the cleaned Player_log.csv s and convert them into relational database tables
# develop locally then deploy to gcp once functional
# Use cloud functions ??

import pandas as pd
import duckdb
import json
import numpy as np
import ast
from collections import Counter

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


# it will return the next hand_id
def insert_turn1_hands(conn, df, hand_id, match_id):

    # used to pull out the player hand objects from the nested payload
    def has_hand_zone(payload_line):
        if payload_line.get('type') != 'GREMessageType_GameStateMessage':
            return False

        gsm = payload_line.get('gameStateMessage')
        if not gsm:
            return False

        return any(
            z.get('type') == 'ZoneType_Hand'
            for z in gsm.get('zones', [])
        )

    # detects the final hand/begining of the actual play phase
    def is_beginning_phase(payload_line):
        if payload_line.get('type') != 'GREMessageType_GameStateMessage':
            return False
        
        gsm = payload_line.get('gameStateMessage')
        if not gsm:
            return False
        
        turn = gsm.get('turnInfo')
        if not turn:
            return False
        
        return turn.get('phase') == 'Phase_Beginning'


    df['payload'] = df['payload'].apply(ast.literal_eval)

#   Getting all the payloads for the hand selection phase
    end_idx = df[df['payload'].apply(is_beginning_phase).values]
    final_hand = end_idx.index[0] + 1
    df_next = df.iloc[:final_hand]

    df_hands = df_next[df_next['payload'].apply(has_hand_zone).values][1:]

#   grabbing some useful values, will be used when writing to the table
    player_id = df_hands.iloc[0]['player_id']
    seatID = df_hands['payload'].iloc[0].get('systemSeatIds')[0]


#   Making a mapping variable to map instanceId of a card to its grpId (arena_id)
    gameObjectMap = {}
    for item in df_hands['payload']:
        for sub_item in item.get('gameStateMessage').get('gameObjects'):
            gameObjectMap[sub_item.get('instanceId')] = sub_item.get('grpId')


#   Puting the contents of the hands into separate columns for easy indexing

    # zoneId = 31 -> player 1 hand
    # zoneId = 35 -> player 2 hand
    # zoneId = 30 -> mulligan retured cards

    def get_zones (payload_line, player):
        all_zones = payload_line.get('gameStateMessage').get('zones')

        for item in all_zones:
            if player == 'p1' and item.get('zoneId') == 31:
                return item.get('objectInstanceIds')
            if player == 'p2' and item.get('zoneId') == 35:
                return item.get('objectInstanceIds')
            if player == 'limbo' and item.get('zoneId') == 30:
                return item.get('objectInstanceIds', '')

    df_hands['hand_p1'] = df_hands['payload'].apply(get_zones, args=('p1',))
    df_hands['hand_p2'] = df_hands['payload'].apply(get_zones, args=('p2',))
    df_hands['hand_limbo'] = df_hands['payload'].apply(get_zones, args=('limbo',))

#   Mapping the grpid (unique card identifier) to the objectInstanceIds (in game object identifier)
#   grpid is eqivalent to arena_id from dim_cards table
    def map_grpid(hand, grpid_map): 
        if hand is None: 
            return None 
        temp = [grpid_map.get(i) for i in hand if i in grpid_map]
        if len(temp) == 0:
            return None
        return temp


    df_hands['hand_p1_grpid'] = df_hands['hand_p1'].apply(map_grpid, args=(gameObjectMap,))
    df_hands['hand_p2_grpid'] = df_hands['hand_p2'].apply(map_grpid, args=(gameObjectMap,))
    df_hands['hand_limbo_grpid'] = df_hands['hand_limbo'].apply(map_grpid, args=(gameObjectMap,))


    # get the relevant player details (for mulliganCount), and put in a column
    def player_details(row):
        players = row['payload'].get('gameStateMessage', {}).get('players', [])

        if row['hand_p1_grpid'] is not None:
            return next(
                (item for item in players if item.get('systemSeatNumber') == 1),
                None
            )
        if row['hand_p2_grpid'] is not None:
            return next(
                (item for item in players if item.get('systemSeatNumber') == 2),
                None
            )
        return None

    df_hands['player'] = df_hands.apply(player_details, axis=1)

#   Formatting the hands_dict for easier writting to table/disk
    hands_dict = []
    last_hand = []
    for index, row in df_hands.iterrows():

        if row['hand_p1_grpid'] is not None:
            # removing the last hand if it is the same and stepping back hand_id (only happens with mulligans)
            if not (Counter(row['hand_p1_grpid']) - Counter(last_hand)):
                hands_dict.pop()
                hand_id -= 1

            hands_dict.append({
                'hand_id':hand_id,
                'init_hand': row['hand_p1_grpid'],
                'mulliganCount': row['player'].get('mulliganCount', 0),
                'limbo_hand': row['hand_limbo_grpid'],
                'player_num': row['player'].get('systemSeatNumber')
            })
            last_hand = row['hand_p1_grpid']

        if row['hand_p2_grpid'] is not None:
            # removing the last hand if it is the same and stepping back hand_id (only happens with mulligans)
            if not (Counter(row['hand_p2_grpid']) - Counter(last_hand)):
                hands_dict.pop()
                hand_id -= 1

            hands_dict.append({
                'hand_id':hand_id,
                'init_hand': row['hand_p2_grpid'],
                'mulliganCount': row['player'].get('mulliganCount', 0),
                'limbo_hand': row['hand_limbo_grpid'],
            })
            last_hand = row['hand_p2_grpid']
        hand_id += 1

#   inserting the rows into the table
    for item in hands_dict:
        went_first = False
        if item.get('player_num') == 1:
            went_first = True

        conn.execute(
            """
            INSERT INTO turn1_hands (hand_id, player_id, match_id, initial_hand, mulliganCount, final_hand, went_first)
            SELECT ?, ?, ?, ?, ?, ?, ?
            """,
            (int(item.get('hand_id')), str(player_id), int(match_id), str(item.get('init_hand')), 
            int(item.get('mulliganCount')), str(item.get('limbo_hand')), str(went_first)
        ))
    return hands_dict.get('hand_id').iloc[-1] +1