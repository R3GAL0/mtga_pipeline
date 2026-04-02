"""
MTGA Pipeline: Silver Layer

This trims and formats the scryfall database to be used as the dim_cards table. 

The scryfall website is a card database for Magic The Gathering, MTG Arena, and MTG Online: https://scryfall.com/
You can download their database here: https://scryfall.com/docs/api/bulk-data
For this project only the Default Cards set is needed

dim_cards needs to be reloaded for each new set release (~7 times per year)

Input: The database default-cards.json file from scryfall

Output: Streaming insert into dim_cards table
"""

import csv
import ijson 
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv('/home/r3gal/develop/mtga_pipeline/.env')

input_path = "/home/r3gal/develop/mtga_pipeline/data/references"

card_list = []

with open(f'{input_path}/default-cards-20260312090730.json', 'rb') as f:

    arena_ids = set()
    for i, obj in enumerate(ijson.items(f, 'item')):
        arena_id = obj.get('arena_id')

        if not arena_id:
            continue
        if arena_id in arena_ids:
            continue
        arena_ids.add(arena_id)

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

        card = {
            "arena_id":     int(obj.get('arena_id')),
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

