{{ config(materialized='table') }}

WITH matches_event_data AS (

    SELECT
        match_id
        , date_time
        , player_trackable_object
        , data
    FROM {{ ref('fct_src__matches_event_data') }}
)

, data_per_minute AS (

    SELECT
        match_id
        , DATE_TRUNC('minute', date_time) AS date_minute
        , player_trackable_object
        , COUNT(*) AS count_per_minute
    FROM matches_event_data
    GROUP BY 1, 2, 3
    
)

, players AS (

    SELECT
        match_id
        , player_id
        , player_name
        , player_team_id
        , player_trackable_object
    FROM {{ ref('clean_src__matches_players') }}

)

, final AS (

    SELECT
        dpm.match_id
        , dpm.date_minute
        , players.player_id
        , players.player_team_id
        , players.player_name
        , players.player_trackable_object
        , dpm.count_per_minute
    FROM data_per_minute AS dpm
    LEFT JOIN players                      ON players.match_id = dpm.match_id
                                            AND players.player_trackable_object = dpm.player_trackable_object

)

SELECT * FROM final
