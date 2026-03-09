
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (
    SELECT
        t1h.player_id,
        t1h.deck_id,
        decks.deck_name,
        t1h.arena_id,
        cards.card_name,
        cards.scryfall_uri,

        NULL as win_rate_draw,      -- to be added later, don't have necessary downstream column/data yet

        (
            SELECT 

            -- grab last hand -> add last hand column to t1hands
            -- UNNEST() last hand
            -- look for arena_id in last hand
            -- join with matches to extract win/loss
            -- compute win/loss totals

        ) as win_rate_opener,    -- add calculation, win% when this card is in the opening hand
        ARRAY_LENGTH(
            ARRAY(
                SELECT card_id
                FROM UNNEST(decks.deck_list) as card_id
                WHERE card_id = t1h.arena_id
            )
        ) as total_in_deck
        --draw_chance         -- add calculation

    FROM {{ source('silver', 'turn1_hands')}} t1h

    LEFT JOIN {{source('silver', 'decks')}} decks
        on t1h.deck_id = decks.deck_id

    LEFT JOIN {{source('silver', 'dim_cards')}} cards
        on t1h.arena_id = cards.arena_id

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
