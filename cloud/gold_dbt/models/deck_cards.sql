"""
Gold layer: deck card summary

Grain:
    deck_id, card_id

Notes:
"""

{{ config(materialized='table') }}


with card_data as (
    SELECT
        decks.deck_id,
        decks.deck_name,
        deck_card, -- arena_id or each card in the deck
        cards.card_name,
        cards.color_identity,
        cards.cmc,
        cards.scryfall_uri,
        cards.set_code,
        cards.set_name

    FROM {{source('mtga_silver', 'decks')}} decks

    CROSS JOIN UNNEST(decks.deck_list) as deck_card

    LEFT JOIN {{source('mtga_silver', 'dim_cards')}} cards
        ON CAST(deck_card AS INT64) = cards.arena_id
),
source_data as (
    SELECT
        deck_id,
        deck_name,
        deck_card,
        card_name,
        color_identity,
        cmc,
        scryfall_uri,
        set_code,
        set_name,
        count(*) as card_quantity
    
    FROM card_data

    GROUP BY 
        deck_id,
        deck_name,
        deck_card,
        card_name,
        color_identity,
        cmc,
        scryfall_uri,
        set_code,
        set_name

)


SELECT * FROM source_data