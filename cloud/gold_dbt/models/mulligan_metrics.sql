"""
Gold layer: Mulligan Aggregate Data

Grain:
    player_id, deck_id, mulligan number (0-7)

Derived metrics:    (per deck)
    - mulligan count per deck
    - mulligan win rate per deck
    - mulligan count per player
    - mulligan win rate per player

Notes:

"""


{{ config(materialized='table') }}

-- getting mulligan counts and wins by player_id and deck_id
with deck_init_hands as (
    SELECT 
        hands.player_id,
        hands.deck_id,
        ANY_VALUE(decks.deck_name) as deck_name,

        COUNTIF(hands.mulliganCount = 0) as mulligan0Count,
        COUNTIF(hands.mulliganCount = 1) as mulligan1Count,
        COUNTIF(hands.mulliganCount = 2) as mulligan2Count,
        COUNTIF(hands.mulliganCount = 3) as mulligan3Count,
        COUNTIF(hands.mulliganCount = 4) as mulligan4Count,
        COUNTIF(hands.mulliganCount = 5) as mulligan5Count,
        COUNTIF(hands.mulliganCount = 6) as mulligan6Count,
        COUNTIF(hands.mulliganCount = 7) as mulligan7Count,
        COUNTIF(hands.mulliganCount = 0 AND hands.player_win) as mulligan0Wins,
        COUNTIF(hands.mulliganCount = 1 AND hands.player_win) as mulligan1Wins,
        COUNTIF(hands.mulliganCount = 2 AND hands.player_win) as mulligan2Wins,
        COUNTIF(hands.mulliganCount = 3 AND hands.player_win) as mulligan3Wins,
        COUNTIF(hands.mulliganCount = 4 AND hands.player_win) as mulligan4Wins,
        COUNTIF(hands.mulliganCount = 5 AND hands.player_win) as mulligan5Wins,
        COUNTIF(hands.mulliganCount = 6 AND hands.player_win) as mulligan6Wins,
        COUNTIF(hands.mulliganCount = 7 AND hands.player_win) as mulligan7Wins
    
    FROM {{source('mtga_silver', 'turn1_hands')}} hands
    LEFT JOIN {{source('mtga_silver', 'decks')}} decks
        ON hands.deck_id = decks.deck_id

    WHERE final_hand
    GROUP BY player_id, deck_id
),
-- grouping by player_id only for player stats
player_init_hands as (
    SELECT 
        player_id,
        NULL as deck_id,

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
    
    -- FROM deck_init_hands hands
    -- GROUP BY player_id
    FROM {{source('mtga_silver', 'turn1_hands')}} hands
    WHERE final_hand
    GROUP BY player_id
),

-- Pivoiting the tables and unioning decks and players, and changing win count to win_pct
deck_hands_pivot as (
    SELECT player_id, deck_id, deck_name, 0 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan0Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan0Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 1 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan1Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan1Wins, mulligan1Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 2 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan2Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan2Wins, mulligan2Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 3 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan3Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan3Wins, mulligan3Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 4 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan4Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan4Wins, mulligan4Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 5 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan5Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan5Wins, mulligan5Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 6 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan6Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan6Wins, mulligan6Count),2) as deck_win_pct FROM deck_init_hands UNION ALL
    SELECT player_id, deck_id, deck_name, 7 as mulligan_count, NULL AS player_game_cnt, NULL as player_win_pct, mulligan7Count AS deck_game_cnt, ROUND(SAFE_DIVIDE(mulligan7Wins, mulligan7Count),2) as deck_win_pct FROM deck_init_hands UNION ALL

    SELECT player_id, deck_id, '' as deck_name, 0 as mulligan_count, mulligan0Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan0Wins, mulligan0Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 1 as mulligan_count, mulligan1Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan1Wins, mulligan1Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 2 as mulligan_count, mulligan2Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan2Wins, mulligan2Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 3 as mulligan_count, mulligan3Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan3Wins, mulligan3Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 4 as mulligan_count, mulligan4Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan4Wins, mulligan4Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 5 as mulligan_count, mulligan5Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan5Wins, mulligan5Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 6 as mulligan_count, mulligan6Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan6Wins, mulligan6Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands UNION ALL
    SELECT player_id, deck_id, '' as deck_name, 7 as mulligan_count, mulligan7Count AS player_game_cnt, ROUND(SAFE_DIVIDE(mulligan7Wins, mulligan7Count),2) as player_win_pct, NULL AS deck_sample_size, NULL as deck_win_count FROM player_init_hands 

)

SELECT * FROM deck_hands_pivot
ORDER BY player_id, deck_id, mulligan_count
