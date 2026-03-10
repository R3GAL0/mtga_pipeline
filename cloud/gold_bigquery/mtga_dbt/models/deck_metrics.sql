
/*
Gold layer: deck summary

Grain:
    deck_id

Derived metrics:    (per deck)
    - Win rate and match counts
    - Mulligan performance
    - Average CMC and CMC curve/histogram
    - Deck color identity

Notes:
    Card arrays from the decks table are exploded to join against dim_cards.
    This may become inefficient at scale and could be replaced by a
    dedicated deck_cards bridge table.
*/

{{ config(materialized='table') }}

with match_data as (
    SELECT
        deck_id,

        AVG(duration_sec) as avg_duration_sec,
        COUNT(*) as total_matches,
        COUNTIF(player_seat = winner_seat) as total_wins

        cmc_curve

    FROM {{source('silver', 'matches')}} mat
    
    GROUP BY deck_id
),
-- Getting the card data from the decks and aggregating
-- exploding arrays may be inefficient for large numbers of decks -> intruduce another downstream table? (deck_cards)
card_data as (
    SELECT
        decks.deck_id,
        deck_card,
        cards.card_name
        cards.color_identity,
        cards.cmc

    FROM {{source('silver', 'decks')}} decks

    CROSS JOIN UNNEST(deck_list) as deck_card

    LEFT JOIN {{source('silver', 'dim_cards')}} cards
        ON CAST(deck_card AS INT64) = cards.arena_id
),
card_agg_data as (
    SELECT 
        deck_id,
        AVG(cmc) as cmc_avg,

        -- cmc_curve, 
        -- The highest mana cards ever printed for MTG was one 16 mana card and a 1 Million cmc card (it is a gimick card)
        -- asside from these 2 cards there are a couple at 15 mana (these are used)
        ARRAY(
            SELECT COUNTIF(LEAST(cmc,16) = x)
            FROM UNNEST(GENERATE_ARRAY(0, 16)) AS x
        ) AS cmc_curve,


        -- deck colors,
        STRING_AGG(DISTINCT char, '' ORDER BY char) AS deck_colors
    
    FROM card_data

    CROSS JOIN UNNEST(SPLIT(color_identity, '')) AS char

    GROUP BY deck_id

),
-- aggregating the mulligan data
mulligan_data as (
    SELECT
        decks.deck_id,

        AVG(t1h.mulliganCount) as avg_mulligans,

        COUNTIF(t1h.mulliganCount = 0) as mulligan0Count,
        COUNTIF(t1h.mulliganCount = 1) as mulligan1Count,
        COUNTIF(t1h.mulliganCount = 2) as mulligan2Count,
        COUNTIF(t1h.mulliganCount = 0 AND t1h.player_win) as mulligan0Wins,
        COUNTIF(t1h.mulliganCount = 1 AND t1h.player_win) as mulligan1Wins,
        COUNTIF(t1h.mulliganCount = 2 AND t1h.player_win) as mulligan2Wins

    FROM {{source('silver', 'decks')}} decks

    LEFT JOIN {{source('silver', 'turn1_hands')}} t1h
        ON decks.deck_id = t1h.deck_id
        and t1h.final_hand
),
-- final calcuations/formatting
source_data as (
    SELECT
        decks.deck_id,
        decks.deck_name,
        decks.player_id,

        pl.display_name,

        -- match data
        mat_d.avg_duration_sec,
        mat_d.total_matches,
        mat_d.total_wins,
        ROUND(SAFE_DIVIDE(mat_d.total_wins, mat_d.total_matches)*100, 2) as win_rate_pct,

        -- mulligan data
        md.avg_mulligans,
        ROUND(SAFE_DIVIDE(md.mulligan0Wins, md.mulligan0Count)*100, 2) as mulligan0_win_rate_pct,
        ROUND(SAFE_DIVIDE(md.mulligan1Wins, md.mulligan1Count)*100, 2) as mulligan1_win_rate_pct,
        ROUND(SAFE_DIVIDE(md.mulligan2Wins, md.mulligan2Count)*100, 2) as mulligan2_win_rate_pct,

        -- card aggregate data
        cad.cmc_avg,
        cad.cmc_curve,
        cad.deck_colors,

    FROM {{source('silver', 'decks')}} decks

    LEFT JOIN {{source('silver', 'players')}} pl
        ON decks.player_id = pl.player_id

    LEFT JOIN match_data mat_d 
        ON decks.deck_id = mat_d.deck_id

    LEFT JOIN mulligan_data md 
        ON decks.deck_id = md.deck_id

    LEFT JOIN card_agg_data cad
        on decks.deck_id = cad.deck_id
)

select *
from source_data

