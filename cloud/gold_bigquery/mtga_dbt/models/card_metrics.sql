
/*
Gold layer: card_opener_stats

Grain:
    player_id, deck_id, arena_id (card)

Purpose:
    Produces card-level analytics measuring how often a card appearing
    in the opening hand correlates with match wins.

Derived features:
    - win_rate_opener: win percentage when card appears in opening hand
    - total_in_deck: number of copies of the card in the deck

Notes:
    Opening hand arrays are exploded with UNNEST to calculate
    statistics at the individual card level.

Possible expansion:
    Add card draws over the game and measure card impact on the game as a function of time of draw

*/

{{ config(materialized='table') }}

-- calculating the win rate of the different opening cards
with opener_cards as (
    SELECT 
        t1h.player_id,
        t1h.deck_id,
        card as arena_id,
        player_win
        -- AVG(player_win) as card_win_frac
    FROM {{ source('mtga_silver', 'turn1_hands')}} t1h
    CROSS JOIN UNNEST(initial_hand) as card
    WHERE final_hand = TRUE
),
opener_stats AS (
    SELECT
        arena_id,
        player_id,
        deck_id,
        ROUND(AVG(CASE WHEN player_win THEN 1 ELSE 0 END)*100, 2) AS win_rate_opener,
        count(*) as drawn_opener_cnt
    FROM opener_cards
    GROUP BY arena_id, player_id, deck_id
),
-- pulling all the data together
source_data as (
    SELECT
        os.player_id,
        os.deck_id,
        decks.deck_name,
        os.arena_id,
        cards.card_name,
        cards.scryfall_uri,

        NULL as win_rate_draw,      -- to be added later, don't have necessary downstream column/data yet

        os.win_rate_opener,    -- win% when this card is in the opening hand
        os.drawn_opener_cnt,   -- number of times this card was drawn in the opener and kept in the final hand

        ARRAY_LENGTH(
            ARRAY(
                SELECT card_id
                FROM UNNEST(decks.deck_list) as card_id
                WHERE card_id = os.arena_id
            )
        ) as total_in_deck      -- total number of this card in the deck

        --draw_chance         -- add calculation

    -- FROM {{ source('mtga_silver', 'turn1_hands')}} t1h
    FROM opener_stats os

    LEFT JOIN {{source('mtga_silver', 'decks')}} decks
        -- on t1h.deck_id = decks.deck_id
        on os.deck_id = decks.deck_id

    LEFT JOIN {{source('mtga_silver', 'dim_cards')}} cards
        -- on t1h.arena_id = cards.arena_id
        on os.arena_id = cards.arena_id

    -- LEFT JOIN opener_stats os
    --     ON t1h.arena_id = os.arena_id

)

select *
from source_data

