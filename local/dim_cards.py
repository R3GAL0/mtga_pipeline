# This makes the dim_cards table
# The default-cards-20260219100953.json was extracted from the scryfall api


import json
import pandas as pd

filetype = "parquet"
# filetype = "csv"

input_path = "/home/r3gal/develop/mtga_pipeline/data/references/default-cards-20260219100953.json"
output_path = f"/home/r3gal/develop/mtga_pipeline/data/references/dim_cards.{filetype}"

rows = []

with open(input_path, "r") as f:
    cards = json.load(f)

for card in cards:
    # card = json.loads(line)

    # Skip cards not on Arena
    if "arena_id" not in card:
        continue

    rows.append({
        "arena_id": card.get("arena_id"),
        "oracle_id": card.get("oracle_id"),
        "name": card.get("name"),
        "released_at": card.get("released_at"),
        "scryfall_uri": card.get("scryfall_uri").split('?')[0],
        "mana_cost": card.get("mana_cost"),
        "cmc": card.get("cmc"),
        "colors": card.get("colors"),
        "color_identity": card.get("color_identity"),
        "type_line": card.get("type_line"),
        "set": card.get("set"),
        "set_name": card.get("set_name"),
        "set_type": card.get("set_type"),
        "rarity": card.get("rarity"),
        "legalities": card.get("legalities")
    })

df = pd.DataFrame(rows)
if filetype == 'parquet':
    df.to_parquet(output_path, index=False)

if filetype == 'csv':
    df.to_csv(output_path, index=False)
