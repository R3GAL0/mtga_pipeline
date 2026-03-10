
/*


*/

{{ config(materialized='table') }}

-- calculating the win rate of the different opening cards
with opener_cards as (
    SELECT 
        card as arena_id,
        player_win
        -- AVG(player_win) as card_win_frac
    FROM {{ source('silver', 'turn1_hands')}}
    CROSS JOIN UNNEST(initial_hand) as card
    WHERE final_hand = TRUE
),
opener_stats AS (
    SELECT
        arena_id,
        AVG(player_win) AS win_rate_opener
    FROM opener_cards
    GROUP BY arena_id
),
-- pulling all the data together
source_data as (
    SELECT
        t1h.player_id,
        t1h.deck_id,
        decks.deck_name,
        t1h.arena_id,
        cards.card_name,
        cards.scryfall_uri,

        NULL as win_rate_draw,      -- to be added later, don't have necessary downstream column/data yet

        os.win_rate_opener,    -- win% when this card is in the opening hand

        ARRAY_LENGTH(
            ARRAY(
                SELECT card_id
                FROM UNNEST(decks.deck_list) as card_id
                WHERE card_id = t1h.arena_id
            )
        ) as total_in_deck      -- total number of this card in the deck

        --draw_chance         -- add calculation

    FROM {{ source('silver', 'turn1_hands')}} t1h

    LEFT JOIN {{source('silver', 'decks')}} decks
        on t1h.deck_id = decks.deck_id

    LEFT JOIN {{source('silver', 'dim_cards')}} cards
        on t1h.arena_id = cards.arena_id

    LEFT JOIN opener_stats os
        ON t1h.arena_id = os.arena_id

)

select *
from source_data

