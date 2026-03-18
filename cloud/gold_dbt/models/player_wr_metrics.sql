
/*
Gold layer: player win rate, broken up by 7 day periods

Grain:
    player_id

Derived Metrics:    (per player)

Notes:
*/

{{ config(materialized='table') }}

-- get win rate and number of games played per player over 7 day periods

with source_data as (
SELECT
    player_id,
    DATE_TRUNC(DATE(start_time), WEEK) AS week_start,
    COUNTIF(player_seat = winner_seat) AS win_cnt_7d,
    COUNT(*) AS game_cnt_7d
FROM {{source('mtga_silver', 'matches')}}
GROUP BY player_id, week_start
)

select *
from source_data
