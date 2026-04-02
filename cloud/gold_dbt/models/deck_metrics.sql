"""
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
    Cards not found in dim_card table will have null cmc values and be 
    excluded from cmc calcs (ie cmc_avg and cmc_curve).
    cmc_avg excludes lands (0 mana cards) and null (cards not found)
"""

{{ config(materialized='table') }}

with match_data as (
    SELECT 
        mat.player_id,
        mat.deck_id, 
        AVG(duration_sec) as avg_duration_sec,
        COUNT(*) as total_matches,
        COUNTIF(player_seat = winner_seat) as total_wins,
        COUNTIF(player_seat = 1 and winner_seat = 1) as play_win_cnt,
        COUNTIF(player_seat = 1) as play_game_cnt,
        COUNTIF(player_seat = 2 and winner_seat = 2) as draw_win_cnt,
        COUNTIF(player_seat = 2) as draw_game_cnt,

        ANY_VALUE(decks.deck_name) AS deck_name,
        DENSE_RANK() OVER (PARTITION BY mat.player_id ORDER BY count(*) DESC) as deck_rank

    FROM {{source('mtga_silver', 'matches')}} mat

    LEFT JOIN {{source('mtga_silver', 'decks')}} decks 
        ON mat.deck_id = decks.deck_id

    GROUP BY mat.player_id, mat.deck_id

),
-- Getting the card data from the decks and aggregating
-- exploding arrays may be inefficient for large numbers of decks -> intruduce another downstream table? (deck_cards)
card_data as (
    SELECT
        decks.deck_id,
        deck_card,
        cards.card_name,
        cards.color_identity,
        cards.cmc

    FROM {{source('mtga_silver', 'decks')}} decks

    CROSS JOIN UNNEST(deck_list) as deck_card

    LEFT JOIN {{source('mtga_silver', 'dim_cards')}} cards
        ON CAST(deck_card AS INT64) = cards.arena_id
),
card_agg_data as (
    -- count cards per cmc bucket
    with cmc_counts as (
        SELECT
            deck_id,
            LEAST(cmc,16) as cmc_bucket,
            COUNT(*) as cnt
        FROM card_data
        GROUP BY deck_id, cmc_bucket
    ),

    -- build the bucket columns for the cmc_curve (looker cant take arrays or pre binned values)
        -- The highest mana cards ever printed for MTG was one 16 mana card and a 1 Million cmc card (it is a gimick card)
        -- asside from these 2 cards there are a couple at 15 mana (these are used)
    cmc_curve as (
        SELECT
            c.deck_id,
            CASE WHEN c.cmc_bucket = 0 then c.cnt ELSE 0 END as cmc_bin_0,
            CASE WHEN c.cmc_bucket = 1 then c.cnt ELSE 0 END as cmc_bin_1,
            CASE WHEN c.cmc_bucket = 2 then c.cnt ELSE 0 END as cmc_bin_2,
            CASE WHEN c.cmc_bucket = 3 then c.cnt ELSE 0 END as cmc_bin_3,
            CASE WHEN c.cmc_bucket = 4 then c.cnt ELSE 0 END as cmc_bin_4,
            CASE WHEN c.cmc_bucket = 5 then c.cnt ELSE 0 END as cmc_bin_5,
            CASE WHEN c.cmc_bucket = 6 then c.cnt ELSE 0 END as cmc_bin_6,
            CASE WHEN c.cmc_bucket = 7 then c.cnt ELSE 0 END as cmc_bin_7,
            CASE WHEN c.cmc_bucket = 8 then c.cnt ELSE 0 END as cmc_bin_8,
            CASE WHEN c.cmc_bucket = 9 then c.cnt ELSE 0 END as cmc_bin_9,
            CASE WHEN c.cmc_bucket = 10 then c.cnt ELSE 0 END as cmc_bin_10,
            CASE WHEN c.cmc_bucket = 11 then c.cnt ELSE 0 END as cmc_bin_11,
            CASE WHEN c.cmc_bucket = 12 then c.cnt ELSE 0 END as cmc_bin_12,
            CASE WHEN c.cmc_bucket = 13 then c.cnt ELSE 0 END as cmc_bin_13,
            CASE WHEN c.cmc_bucket = 14 then c.cnt ELSE 0 END as cmc_bin_14,
            CASE WHEN c.cmc_bucket = 15 then c.cnt ELSE 0 END as cmc_bin_15,
            CASE WHEN c.cmc_bucket = 16 then c.cnt ELSE 0 END as cmc_bin_16

            -- ARRAY_AGG(IFNULL(c.cnt,0) ORDER BY cmc_val) as cmc_curve

        FROM cmc_counts c
        -- (
        --     SELECT DISTINCT deck_id FROM card_data
        -- ) d
        -- -- CROSS JOIN UNNEST(GENERATE_ARRAY(0,16)) as cmc_val
        -- LEFT JOIN cmc_counts c
        --     ON d.deck_id = c.deck_id
        --     -- AND cmc_val = c.cmc_bucket

        -- GROUP BY deck_id
    ),
    -- getting the unique colors per deck. 
        -- color_identity is unique per card and is a string ie BW for black and white
    color_identity as (
        SELECT
            deck_id,
            STRING_AGG(DISTINCT char, '' ORDER BY char) as deck_colors
        FROM card_data
        CROSS JOIN UNNEST(SPLIT(color_identity, '')) as char
        GROUP BY deck_id
    )

    SELECT
        c.deck_id,
        -- skipping lands and missing cards from cmc_avg
        AVG(CASE WHEN c.cmc > 0 then c.cmc END) as cmc_avg,
        
        Max(cc.cmc_bin_0) as cmc_bin_0,
        Max(cc.cmc_bin_1) as cmc_bin_1,
        Max(cc.cmc_bin_2) as cmc_bin_2,
        Max(cc.cmc_bin_3) as cmc_bin_3,
        Max(cc.cmc_bin_4) as cmc_bin_4,
        Max(cc.cmc_bin_5) as cmc_bin_5,
        Max(cc.cmc_bin_6) as cmc_bin_6,
        Max(cc.cmc_bin_7) as cmc_bin_7,
        Max(cc.cmc_bin_8) as cmc_bin_8,
        Max(cc.cmc_bin_9) as cmc_bin_9,
        Max(cc.cmc_bin_10) as cmc_bin_10,
        Max(cc.cmc_bin_11) as cmc_bin_11,
        Max(cc.cmc_bin_12) as cmc_bin_12,
        Max(cc.cmc_bin_13) as cmc_bin_13,
        Max(cc.cmc_bin_14) as cmc_bin_14,
        Max(cc.cmc_bin_15) as cmc_bin_15,
        Max(cc.cmc_bin_16) as cmc_bin_16,

        ci.deck_colors
    FROM card_data c
    LEFT JOIN cmc_curve cc
        ON c.deck_id = cc.deck_id
    LEFT JOIN color_identity ci
        ON c.deck_id = ci.deck_id
    GROUP BY c.deck_id, ci.deck_colors

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

    FROM {{source('mtga_silver', 'decks')}} decks

    LEFT JOIN {{source('mtga_silver', 'turn1_hands')}} t1h
        ON decks.deck_id = t1h.deck_id
        and t1h.final_hand
    
    GROUP BY decks.deck_id
),
player_wr as (
    SELECT  
        player_id,
        SAFE_DIVIDE(COUNTIF(player_seat = winner_seat), COUNT(*)) AS player_wr
    
    FROM {{source('mtga_silver', 'matches')}}
    GROUP BY player_id
),
-- final calcuations/formatting
source_data as (
    SELECT
        decks.deck_id,
        decks.deck_name,
        decks.player_id,

        pl.display_name,

        -- match data
        ROUND(mat_d.avg_duration_sec, 2) as avg_duration_sec,
        mat_d.total_matches,
        mat_d.total_wins,
        mat_d.play_game_cnt as total_matches_play,
        mat_d.draw_game_cnt as total_matches_draw,

        ROUND(SAFE_DIVIDE(mat_d.total_wins, mat_d.total_matches)*100, 2) as win_rate_pct,
        ROUND(SAFE_DIVIDE(mat_d.play_win_cnt, mat_d.play_game_cnt)*100, 2) as win_rate_play,
        ROUND(SAFE_DIVIDE(mat_d.draw_win_cnt, mat_d.draw_game_cnt)*100, 2) as win_rate_draw,
        mat_d.deck_rank,


        -- power score (k = 50)
            -- adjusted_wr = (wins+k*global_wr)/(games+k)
            -- power_score = adjusted_wr * log(games)
            -- where k = confidence weight (~50-100 games)
        ROUND(SAFE_DIVIDE( -- adjusted_wr
                mat_d.total_wins + 50 * pwr.player_wr, 
                (mat_d.total_matches+50)
            )*LOG(10 + mat_d.total_matches),2) 
            as total_power_score,

        ROUND(SAFE_DIVIDE( -- adjusted_wr
                mat_d.play_win_cnt + 50 * pwr.player_wr, 
                (mat_d.play_game_cnt+50)
            )*LOG(10 + mat_d.total_matches),2) 
            as play_power_score,

        ROUND(SAFE_DIVIDE( -- adjusted_wr
                mat_d.draw_win_cnt + 50 * pwr.player_wr, 
                (mat_d.draw_game_cnt+50)
            )*LOG(10 + mat_d.total_matches),2) 
            as draw_power_score,


        -- mulligan data
        ROUND(md.avg_mulligans, 2) as avg_mulligans,
        ROUND(SAFE_DIVIDE(md.mulligan0Wins, md.mulligan0Count)*100, 2) as mulligan0_win_rate_pct,
        md.mulligan0Count,
        ROUND(SAFE_DIVIDE(md.mulligan1Wins, md.mulligan1Count)*100, 2) as mulligan1_win_rate_pct,
        md.mulligan1Count,
        ROUND(SAFE_DIVIDE(md.mulligan2Wins, md.mulligan2Count)*100, 2) as mulligan2_win_rate_pct,
        md.mulligan2Count,

        -- card aggregate data
        ROUND(cad.cmc_avg, 2) as cmc_avg,
        -- cad.cmc_curve,
        -- cad.cmc_bin_0,
        -- cad.cmc_bin_1,
        -- cad.cmc_bin_2,
        -- cad.cmc_bin_3,
        -- cad.cmc_bin_4,
        -- cad.cmc_bin_5,
        -- cad.cmc_bin_6,
        -- cad.cmc_bin_7,
        -- cad.cmc_bin_8,
        -- cad.cmc_bin_9,
        -- cad.cmc_bin_10,
        -- cad.cmc_bin_11,
        -- cad.cmc_bin_12,
        -- cad.cmc_bin_13,
        -- cad.cmc_bin_14,
        -- cad.cmc_bin_15,
        -- cad.cmc_bin_16,
        cad.deck_colors

    FROM {{source('mtga_silver', 'decks')}} decks

    LEFT JOIN {{source('mtga_silver', 'players')}} pl
        ON decks.player_id = pl.player_id

    LEFT JOIN match_data mat_d 
        ON decks.deck_id = mat_d.deck_id

    LEFT JOIN mulligan_data md 
        ON decks.deck_id = md.deck_id

    LEFT JOIN card_agg_data cad
        on decks.deck_id = cad.deck_id

    LEFT JOIN player_wr pwr
        ON decks.player_id = pwr.player_id
)
-- PASSED
-- select * from match_data
-- select * from card_data
-- select * from card_agg_data

-- CHECKING
-- select * from mulligan_data

select * from source_data

