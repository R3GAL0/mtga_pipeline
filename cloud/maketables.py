# take the cleaned Player_log.csv s and convert them into relational database tables
# develop locally then deploy to gcp once functional
# Use cloud functions ??

import pandas as pd


# read each csv 1 game at a time and partition into the correct tables


# what tables do I want?

# players table
    # player_id
    # display_name
    # region ??

# cards table 
#     card_id
#     card_name
#     card_type (land, creature, sorcery, instant)
#     mana_cost (2BB ->  2 colorless, 2 black mana)
#         how to do variable mana (ie pay BB, UU, or BU)??
#     card_set (which set the card was released in)
#     legal_formats
#     card_color (UWBGR, C) -> C = colorless

# decks table
#     deck_id
#     player_id
#     match_id
#     format (what formats the deck is for)

# deck_cards
#     deck_id
#     card_id
#     quantity

# matches table (1 row per match)
#     match_id 
#     timestamp 
#     winner_id
#     loser_id
#     first_player_id
#     format

# turn1_hands table (1 row for inital hand and a second/third for each mulligan)
#     match_id
#     player_id
#     inital_hand
#     mulligans
#     final_hand
#     went_first

# rank_progression table (optional/stretch goal)
#     player_id
#     timestamp
#     rank_tier
#     rank_sub_tier