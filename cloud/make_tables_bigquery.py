"""
MTGA Data Pipeline: Silver Layer Ingestion

take the cleaned Player_log.csv and insert the relevant values into bigquery tables

GENERAL PROCESS:
    add a new player to players if no match was found for player_id

    For each game:

    insert the deck_list in decks, and a deck_list hash for uniqueness
    insert the match details in matches
      card draws -> draw_order
    insert turn 1 hands with proper mappings from objectInstanceIds to oracle_ids
      including extra rows for mulligans
"""

import pandas as pd
import json
from collections import Counter
import os
from google.cloud import bigquery
import hashlib
import datetime


# def insert_all(data_dir, db_dir):
def insert_all(data_dir, client):

    file_list = os.listdir(path=data_dir)

    for file in file_list:
        print("Processing: " + file)
        
        df_temp = pd.read_csv(f"{data_dir}/{file}")

        # the truncated payloads break json.loads -> dropping these rows
        df_temp = df_temp[df_temp["payload"] != '[Message summarized because one or more GameStateMessages exceeded the 50 GameObject or 50 Annotation limit.]']

        df_temp['payload'] = df_temp['payload'].apply(json.loads)
        df = df_temp.explode('payload').reset_index(drop=True)

        for game_num, df_part in df.groupby('game_num'):
            insert_player(client, df_part)
            match_id = insert_match(client, df_part)
            insert_turn1_hands(client, df_part, match_id)

# increments the pk_counter table, returns the incremented PK value
# used to get the next PK for each table (BigQuery doesnt have autoincrement on PKs)
def increment_id(client, pk_name):

    update_query = """
    UPDATE `mtgapipeline.mtga_silver.pk_counter`
    SET current_value = current_value + 1
    WHERE pk_name = @pk_name
    """
    select_query = """
    SELECT current_value
    FROM `mtgapipeline.mtga_silver.pk_counter`
    WHERE pk_name = @pk_name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pk_name", "STRING", pk_name),
        ]
    )

    # update
    client.query(update_query, job_config=job_config).result()

    # get new value
    row = next(client.query(select_query, job_config=job_config).result())
    return row.current_value

# hashes a list, for comparing uniqueness of lists (insert_deck)
def hash_list(list):
    list_json = json.dumps(list, sort_keys=True)
    return hashlib.sha256(list_json.encode("utf-8")).hexdigest()


# returns the deck_id, of the new deck or matching old deck
def insert_deck (client, df, match_id):

    player_id = df.iloc[0]['player_id']
    deck_obj = df.iloc[0]['payload'].get('request') 

    try:
        nested = json.loads(deck_obj)

        deck_name = nested.get('Summary').get('Name')
        deck_list_temp = nested.get('Deck').get('MainDeck')
        deck_sideboard = nested.get('Deck').get('Sideboard')
        deck_commander_temp = nested.get('Deck').get('CommandZone')

        if len(deck_commander_temp) == 0:
            deck_commander = ''
        else:
            deck_commander = deck_commander_temp[0]

        # need to flatten deck_list

        deck_list = []
        for item in deck_list_temp:
            for _ in range(item.get('quantity')):
                deck_list.append(item.get('cardId'))
        deck_list = sorted(deck_list)

    except Exception as error_details:
        print('insert_deck error: ' + str(error_details))
        deck_name = 'Blank'
        deck_list = []
        deck_sideboard = []
        deck_commander = ''

    #   checking if the deck already exists, returning the deck_id if it does
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("deck_hash", "STRING", hash_list(deck_list)),
            bigquery.ScalarQueryParameter("side_hash", "STRING", hash_list(deck_sideboard)),
            ]
    )
    # deck_list and deck_sideboard are lists of oracle_ids

    query_deck = """
        SELECT deck_id from `mtgapipeline.mtga_silver.decks`
        WHERE deck_hash = @deck_hash
        and side_hash = @side_hash
        """

    query_job_deck = next(client.query(query_deck, job_config=job_config).result(), None)
    if query_job_deck is not None:
        return query_job_deck.deck_id

    deck_id = increment_id(client, 'deck_id')

    row_to_insert = [
        {
            "deck_id": deck_id,
            "player_id": player_id,
            "match_id": match_id,
            "set_code": "",
            "deck_name": deck_name,
            "deck_list": deck_list,
            "deck_sideboard": deck_sideboard,
            "deck_commander": deck_commander,
            "deck_hash": hash_list(deck_list),
            "side_hash": hash_list(deck_sideboard)
        }
    ]
    errors = client.insert_rows_json("mtgapipeline.mtga_silver.decks", row_to_insert)
    if errors:
        print("Deck insert errors: " + str(errors))

    return deck_id

# attempts to insert player, will skip if player_id is non_unique
def insert_player (client, df):

    player_id = df.iloc[0]['player_id']

    players = df.iloc[-1]['payload'].get('gameRoomConfig').get('reservedPlayers')
    display_name = ''
    for item in players:
        if item.get('userId') == player_id:
            display_name = item.get('playerName')


    query_player = """
        SELECT player_id from `mtgapipeline.mtga_silver.players`
        WHERE player_id = @player_id
        """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_id", "STRING", player_id)
            ]
    )

    query_job_player = next(client.query(query_player, job_config=job_config).result(), None)
    if query_job_player is not None:
        return
        
    row_to_insert = [
        {
            "player_id": player_id,
            "display_name": display_name
        }
    ]
    errors = client.insert_rows_json("mtgapipeline.mtga_silver.players", row_to_insert)
    if errors:
        print("Players insert errors: " + str(errors))


# it will insert all the hands for the game with unique hand_ids
def insert_turn1_hands(client, df, match_id):

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
        if str(type(payload_line)) != "<class 'dict'>":
            return False
        if payload_line.get('type') != 'GREMessageType_GameStateMessage':
            return False
        
        gsm = payload_line.get('gameStateMessage')
        if not gsm:
            return False
        
        turn = gsm.get('turnInfo')
        if not turn:
            return False
        
        return turn.get('phase') == 'Phase_Beginning'

    # slicing the df to get the initial hand payloads
    beginning_idx = df[df['payload'].apply(is_beginning_phase)].index.min()
    df_until_beginning = df.loc[:beginning_idx]
    df_hands = df_until_beginning[df_until_beginning['payload'].apply(has_hand_zone)].iloc[1:]

    #   grabbing some useful values, will be used when writing to the table
    player_id = df_hands.iloc[0]['player_id']
    seatID = df_hands['payload'].iloc[0].get('systemSeatIds')[0]

    #   Making a mapping variable to map instanceId of a card to its grpId (arena_id)
    gameObjectMap = {}
    for item in df_hands['payload']:
        game_state = item.get('gameStateMessage')
        if not game_state:
            continue

        game_objects = game_state.get('gameObjects')
        if not game_objects:
            continue

        for sub_item in game_objects:
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
    #   grpid is eqivalent to arena_id/oracle_id from dim_cards table
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

    #   Formatting the hands_dict for writting to disk
    hands_dict = []
    last_hand = []
    mulliganCount = 0
    player_num = None
    for index, row in df_hands.iterrows():

        if row.get('player', {}) is not None:
            mulliganCount = row.get('player', {}).get('mulliganCount', 0)

        player = row.get('player', {})
        if player_num is None:
            player_num = player.get('systemSeatNumber', 0)

        if row['hand_p1_grpid'] is not None:
            # getting a new hand_id for insertion
            hand_id = increment_id(client, 'hand_id')

            hands_dict.append({
                'hand_id':hand_id,
                'player_id': player_id,
                'match_id': match_id,
                'initial_hand': row['hand_p1_grpid'],
                'mulliganCount': mulliganCount,
                'discarded': row['hand_limbo_grpid'] if row['hand_limbo_grpid'] is not None else [],
                'went_first': True
            })
            last_hand = row['hand_p1_grpid']

        if row['hand_p2_grpid'] is not None:
            # getting a new hand_id for insertion
            hand_id = increment_id(client, 'hand_id')

            hands_dict.append({
                'hand_id':hand_id,
                'player_id': player_id,
                'match_id': match_id,
                'initial_hand': row['hand_p2_grpid'],
                'mulliganCount': mulliganCount,
                'discarded': row['hand_limbo_grpid'] if row['hand_limbo_grpid'] is not None else [],
                'went_first': False
            })
            last_hand = row['hand_p2_grpid']\

    #   inserting the rows into the table

    errors = client.insert_rows_json("mtgapipeline.mtga_silver.turn1_hands", hands_dict)
    if errors:
        print("T1 hands insert errors: " + str(errors))


# inserts a match into the db
# returns a match_id for the match
# executes the insert_deck function
def insert_match (client, df):

    match_id = increment_id(client, 'match_id')

    player_id = df.iloc[0]['player_id']
    deck_id = insert_deck(client, df, match_id)

    df['timestamp_f'] =  pd.to_datetime(
        df['timestamp'],
        format='%m/%d/%Y %I:%M:%S %p'
        )
    start_time = df.iloc[0]['timestamp_f'].tz_localize('America/Toronto').tz_convert('UTC').isoformat()

    duration = df.iloc[-1]['timestamp_f'] - df.iloc[0]['timestamp_f']
    duration_sec = int(duration.total_seconds())

    # Certain events will exclude the deck_list payload (ie Jump-In)
    try:
        attributes = json.loads(df['payload'].iloc[0]['request']).get('Summary').get('Attributes')
        game_format = next(
            (attr['value'] for attr in attributes if attr['name'] == 'Format'),
            None
        )
    except:
        game_format = 'Event'

    player_seat = 0
    players = df['payload'].iloc[-1].get('gameRoomConfig').get('reservedPlayers')
    
    for item in players:
        if item.get('userId') == player_id:
            player_seat = item.get('systemSeatId')

    # 'MatchScope_Game' -> Is the result of one game in a match (can be 1 or 3 games per match)
    match_results = df['payload'].iloc[-1].get('finalMatchResult').get('resultList')
    winner_seat = 0
    for item in match_results:
        if item.get('scope') == 'MatchScope_Match':
            winner_seat = item.get('winningTeamId')

    # will change draw_order after implementation
    draw_order = [
        {'cards': []}
    ]
    match_dict = [{
        'match_id': match_id,
        'deck_id': deck_id,
        'player_id': player_id,
        'player_seat': player_seat,
        'start_time': start_time,
        'duration_sec': duration_sec,
        'winner_seat': winner_seat,
        'game_format': game_format,
        'draw_order': draw_order
    }]

    errors = client.insert_rows_json("mtgapipeline.mtga_silver.matches", match_dict)
    if errors:
        print("Matches insert errors: " + str(errors))
        
    return match_id

if __name__ == "__main__":
    insert_all()
    