
/*
Gold layer: player summary

Grain:
    player_id

Derived Metrics:    (per player)
    - Match performance metrics
    - Rolling 30-day activity and win rate
    - Mulligan behavior statistics
    - Most frequently played deck

Notes:
    Rolling match statistics are calculated using window functions
*/

{{ config(materialized='table') }}

-- grabbing some deck usage stats
with deck_usage as (
    SELECT 
        mat.deck_id, 
        mat.player_id,
        count(*) as match_cnt,
        ANY_VALUE(decks.deck_name) AS deck_name

    FROM {{source('mtga_silver', 'matches')}} mat

    LEFT JOIN {{source('mtga_silver', 'decks')}} decks 
        ON mat.deck_id = decks.deck_id

    GROUP BY mat.player_id, mat.deck_id

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id
        ORDER BY match_cnt DESC
    ) = 1
),
-- most_played_deck as (
--     SELECT *
--     FROM deck_usage
-- ),
-- rolling up match data to player level
match_window as (
    SELECT 
        mat.player_id,
        mat.start_time,

        count(*) OVER (
            PARTITION BY player_id
            -- ORDER BY mat.start_time
            -- RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
            ORDER BY TIMESTAMP_DIFF(mat.start_time, TIMESTAMP '2025-01-01', SECOND) ASC
            RANGE BETWEEN 36000 PRECEDING AND CURRENT ROW
        ) AS matches_30d,

        COUNTIF(mat.winner_seat = mat.player_seat) OVER (
            PARTITION BY player_id
            -- ORDER BY mat.start_time
            -- RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
            ORDER BY TIMESTAMP_DIFF(mat.start_time, TIMESTAMP '2025-01-01', SECOND) ASC
            RANGE BETWEEN 36000 PRECEDING AND CURRENT ROW
        ) AS total_wins_30d

    FROM {{source('mtga_silver', 'matches')}} mat

    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY player_id
        ORDER BY start_time DESC 
    ) = 1
),
init_hands as (
    SELECT 
        player_id,
        -- match_id,

        COUNTIF(mulliganCount = 0) as mulligan0Count,
        COUNTIF(mulliganCount = 1) as mulligan1Count,
        COUNTIF(mulliganCount = 2) as mulligan2Count,
        COUNTIF(mulliganCount = 0 AND player_win) as mulligan0Wins,
        COUNTIF(mulliganCount = 1 AND player_win) as mulligan1Wins,
        COUNTIF(mulliganCount = 2 AND player_win) as mulligan2Wins,

        -- COUNTIF(hands.mulliganCount = 0) as mulligan0Count,
        -- COUNTIF(hands.mulliganCount = 1) as mulligan1Count,
        -- COUNTIF(hands.mulliganCount = 2) as mulligan2Count,
        -- COUNTIF(hands.mulliganCount = 0 AND hands.player_win) as mulligan0Wins,
        -- COUNTIF(hands.mulliganCount = 1 AND hands.player_win) as mulligan1Wins,
        -- COUNTIF(hands.mulliganCount = 2 AND hands.player_win) as mulligan2Wins,

        AVG(mulliganCount) as avg_mulligans
    
    FROM {{source('mtga_silver', 'turn1_hands')}} hands

    WHERE final_hand
    GROUP BY player_id

    -- QUALIFY ROW_NUMBER() OVER(
    --     PARTITION BY player_id
    --     ORDER BY hands.hand_id DESC 
    -- ) = 1

),
match_data as (
    SELECT
        mat.player_id,
        count(*) as total_matches,
        COUNTIF(mat.winner_seat = mat.player_seat)AS total_wins,

        -- rolling matches 30d
        MAX(mat_win.matches_30d) as matches_30d,
        MAX(mat_win.total_wins_30d) as total_wins_30d,

        -- showing window functions instead of using below method

        -- COUNTIF(
        --     mat.start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        -- ) AS matches_30d,
        -- COUNTIF(
        --     mat.start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        --     AND mat.winner_seat = mat.player_seat
        -- ) AS total_wins_30d,        

        -- mulligan winrate 0,1,2
        MAX(hands.mulligan0Count) as mulligan0Count,
        MAX(hands.mulligan1Count) as mulligan1Count,
        MAX(hands.mulligan2Count) as mulligan2Count,
        MAX(hands.mulligan0Wins) as mulligan0Wins,
        MAX(hands.mulligan1Wins) as mulligan1Wins,
        MAX(hands.mulligan2Wins) as mulligan2Wins,

        MAX(hands.avg_mulligans) as avg_mulligans,
        -- COUNTIF(hands.mulliganCount = 0) as mulligan0Count,
        -- COUNTIF(hands.mulliganCount = 1) as mulligan1Count,
        -- COUNTIF(hands.mulliganCount = 2) as mulligan2Count,
        -- COUNTIF(hands.mulliganCount = 0 AND hands.player_win) as mulligan0Wins,
        -- COUNTIF(hands.mulliganCount = 1 AND hands.player_win) as mulligan1Wins,
        -- COUNTIF(hands.mulliganCount = 2 AND hands.player_win) as mulligan2Wins,

        -- averages
        AVG(mat.duration_sec) as avg_duration_sec,

        -- most played deck id, name
        ANY_VALUE(mpd.deck_id) as most_played_deck_id, 
        ANY_VALUE(mpd.deck_name) as most_played_deck_name,

        -- time last played
        MAX(mat.start_time) as time_last_played


    FROM {{source('mtga_silver', 'matches')}} mat

    -- LEFT JOIN {{source('mtga_silver', 'turn1_hands')}} hands
    LEFT JOIN init_hands hands
        -- ON mat.match_id = hands.match_id
        ON mat.player_id = hands.player_id -- shouldn't be needed but adding anyways

    LEFT JOIN deck_usage mpd
        ON mat.player_id = mpd.player_id

    LEFT JOIN match_window mat_win
        -- ON mat.match_id = mat_win.match_id
        ON mat.player_id = mat_win.player_id

    GROUP BY mat.player_id

),
source_data as (
    SELECT
        md.player_id,
        pl.display_name,

        md.total_matches,
        md.total_wins,
        md.total_matches - md.total_wins as total_losses,
        ROUND(SAFE_DIVIDE(md.total_wins, md.total_matches)*100, 2) as win_rate_pct,
        ROUND(SAFE_DIVIDE(md.total_wins_30d, md.matches_30d)*100, 2) as win_rate_30d_pct,

        md.avg_duration_sec,
        md.avg_mulligans,

        ROUND(SAFE_DIVIDE(md.mulligan0Wins, md.mulligan0Count)*100, 2) as mulligan0_win_rate_pct,
        ROUND(SAFE_DIVIDE(md.mulligan1Wins, md.mulligan1Count)*100, 2) as mulligan1_win_rate_pct,
        ROUND(SAFE_DIVIDE(md.mulligan2Wins, md.mulligan2Count)*100, 2) as mulligan2_win_rate_pct,

        md.most_played_deck_id,
        md.most_played_deck_name,
        md.time_last_played


    FROM match_data md

    LEFT JOIN {{source('mtga_silver', 'players')}} pl
        ON md.player_id = pl.player_id
)

select *
from source_data
