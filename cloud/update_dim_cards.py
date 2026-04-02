"""
MTGA Pipeline: Silver Layer

The scryfall database is not complete. It has some card data, but not the associated arena_ids for matching. 
This program adds a couple of the mising cards to the dim_cards table

The scryfall website is a card database for Magic The Gathering, MTG Arena, and MTG Online: https://scryfall.com/
You can download their database here: https://scryfall.com/docs/api/bulk-data
For this project only the Default Cards set is needed


Input: 
    The database default-cards.json file from scryfall
    The missing cards oracle_id and arena_id

Output: Streaming insert into dim_cards table

Notes:
    This problem is prominent for card styles, as they can result in different arena_ids
"""

miss_cards = [
    { # Timeline Culler
        'oracle_id': "3c307122-bc18-41f1-87d5-ae6b3ca9b8b9",
        'arena_id': 96695
    },
    { # Requiting Hex
        'oracle_id': "eda16b31-33db-4ad2-901a-80fbe6273733",
        'arena_id': 98436
    },
    { # Dark Confidant
        'oracle_id': "2068185c-1b50-47d0-aa3f-bf505d199428",
        'arena_id': 95953
    },
    { # Day of Black Sun
        'oracle_id': "dc857129-533c-41d1-8e8b-1c0443030d69",
        'arena_id': 97369
    },
    { # Cecil, Dark Knight // Cecil, Redeemed Paladin
        'oracle_id': "e4719557-d94e-490f-8a56-c1867a01d2ef",
        'arena_id': 96696
    },
    { # Demon Wall
        'oracle_id': "077b5ed2-05c2-4a16-b5eb-794884ee1c21",
        'arena_id': 95949
    },
    { # Swamp
        'oracle_id': "56719f6a-1a6c-4c0a-8d21-18f7d7350b68",
        'arena_id': 34716
    },
    { # Mountain
        'oracle_id': "a3fb7228-e76b-4e96-a40e-20b5fed75685",
        'arena_id': 34742
    },
    { # Mountain
        'oracle_id': "a3fb7228-e76b-4e96-a40e-20b5fed75685",
        'arena_id': 98594
    }
]




import csv
import ijson 
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv('/home/r3gal/develop/mtga_pipeline/.env')

input_path = "/home/r3gal/develop/mtga_pipeline/data/references"

card_list = []

with open(f'{input_path}/default-cards-20260312090730.json', 'rb') as f:
    
    arena_ids = set()
    miss_oracle_id = [c.get("oracle_id") for c in miss_cards]

    for i, obj in enumerate(ijson.items(f, 'item')):
        oracle_id = obj.get('oracle_id')

        # if not arena_id:
        #     continue
        if oracle_id not in miss_oracle_id:
            continue
        
        arena_ids = []
        for item in miss_cards:
            if item.get('oracle_id') == oracle_id:
                arena_ids.append(item.get('arena_id'))

        # both colors and color_identity are lists. Flattening for storage
        color_l = obj.get('colors')
        colors = ''
        if color_l:
            for char in color_l:
                colors += char

        colorIden_l = obj.get('color_identity')
        colorIden = ''
        if colorIden_l:
            for char in colorIden_l:
                colorIden += char

        for arena_id in arena_ids:
            card = {
                "arena_id":     int(arena_id),
                "oracle_id":    obj.get('oracle_id'),
                "card_name":    obj.get('name'),
                "scryfall_uri": obj.get('scryfall_uri').split('?')[0],
                "mana_cost":    obj.get('mana_cost'),
                "cmc":          int(obj.get('cmc')),
                "colors":       colors,  
                "color_identity":   colorIden, 
                "type_line":    obj.get('type_line'),
                "set_code":     obj.get('set'),
                "set_name":     obj.get('set_name'),
                "set_type":     obj.get('set_type'),
                "rarity":       obj.get('rarity')
            }
            card_list.append(card)


# streaming to BigQuery

client = bigquery.Client()
table_id = 'mtgapipeline.mtga_silver.dim_cards'

errors = client.insert_rows_json(table_id, card_list)
if errors:
    print(errors)

