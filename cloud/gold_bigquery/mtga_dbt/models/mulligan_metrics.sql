/*

*/

-- final columns (deck_id will be null for full player data)
-- player_id, deck_id, mulligan_count (0-7), player_win_rate, player_sample_size, deck_win_rate, deck_sample_size

{{ config(materialized='table') }}

-- getting mulligan counts and wins by player_id and deck_id
with deck_init_hands as (
    SELECT 
        player_id,
        deck_id,

        COUNTIF(mulliganCount = 0) as mulligan0Count,
        COUNTIF(mulliganCount = 1) as mulligan1Count,
        COUNTIF(mulliganCount = 2) as mulligan2Count,
        COUNTIF(mulliganCount = 3) as mulligan3Count,
        COUNTIF(mulliganCount = 4) as mulligan4Count,
        COUNTIF(mulliganCount = 5) as mulligan5Count,
        COUNTIF(mulliganCount = 6) as mulligan6Count,
        COUNTIF(mulliganCount = 7) as mulligan7Count,
        COUNTIF(mulliganCount = 0 AND player_win) as mulligan0Wins,
        COUNTIF(mulliganCount = 1 AND player_win) as mulligan1Wins,
        COUNTIF(mulliganCount = 2 AND player_win) as mulligan2Wins,
        COUNTIF(mulliganCount = 3 AND player_win) as mulligan3Wins,
        COUNTIF(mulliganCount = 4 AND player_win) as mulligan4Wins,
        COUNTIF(mulliganCount = 5 AND player_win) as mulligan5Wins,
        COUNTIF(mulliganCount = 6 AND player_win) as mulligan6Wins,
        COUNTIF(mulliganCount = 7 AND player_win) as mulligan7Wins
    
    FROM {{source('mtga_silver', 'turn1_hands')}} hands
    WHERE final_hand
    GROUP BY player_id, deck_id
),
-- grouping by player_id only for player stats
player_init_hands as (
    SELECT 
        player_id,
        NULL as deck_id,

        count(mulligan0Count) as mulligan0Count,
        count(mulligan1Count) as mulligan1Count,
        count(mulligan2Count) as mulligan2Count,
        count(mulligan3Count) as mulligan3Count,
        count(mulligan4Count) as mulligan4Count,
        count(mulligan5Count) as mulligan5Count,
        count(mulligan6Count) as mulligan6Count,
        count(mulligan7Count) as mulligan7Count,

        count(mulligan0Wins) as mulligan0Wins,
        count(mulligan1Wins) as mulligan1Wins,
        count(mulligan2Wins) as mulligan2Wins,
        count(mulligan3Wins) as mulligan3Wins,
        count(mulligan4Wins) as mulligan4Wins,
        count(mulligan5Wins) as mulligan5Wins,
        count(mulligan6Wins) as mulligan6Wins,
        count(mulligan7Wins) as mulligan7Wins
    
    FROM deck_init_hands hands
    GROUP BY player_id
),

-- Pivoiting the tables and unioning decks and players, and changing win count to win_pct
deck_hands_pivot as (
    SELECT player_id, deck_id, 0 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan0Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan0Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 1 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan1Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan1Wins, mulligan1Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 2 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan2Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan2Wins, mulligan2Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 3 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan3Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan3Wins, mulligan3Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 4 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan4Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan4Wins, mulligan4Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 5 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan5Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan5Wins, mulligan5Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 6 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan6Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan6Wins, mulligan6Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, 7 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan7Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan7Wins, mulligan7Count)*100,2) as deck_win_pct FROM deck_init_hands UNION ALL

    SELECT player_id, deck_id, 0 as mulligan_count, mulligan0Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan0Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 1 as mulligan_count, mulligan1Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan1Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 2 as mulligan_count, mulligan2Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan2Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 3 as mulligan_count, mulligan3Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan3Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 4 as mulligan_count, mulligan4Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan4Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 5 as mulligan_count, mulligan5Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan5Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 6 as mulligan_count, mulligan6Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan6Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, 7 as mulligan_count, mulligan7Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan7Count)*100,2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands 

)

SELECT * FROM deck_hands_pivot
ORDER BY player_id, deck_id, mulligan_count

-- -- final calcuations/formatting

-- -- player_id, deck_id, mulligan_count (0-7), player_win_rate, player_sample_size, deck_win_rate, deck_sample_size
-- source_data as (
--     SELECT
--         COALESCE(ph.player_id, dh.player_id) as player_id,

--     FROM player_init_hands ph
-- )