{{ config(materialized='table') }}

WITH final AS (

    SELECT
        id                                  AS match_id
        , player ->> 'id'                   AS player_id
        , player ->> 'short_name'           AS player_name
        , player ->> 'team_id'              AS player_team_id
        , player ->> 'goal'                 AS player_goal
        , player ->> 'trackable_object'     AS player_trackable_object
    FROM {{ ref('stg_src__matches') }}
    , JSONB_ARRAY_ELEMENTS(players) AS player
)

SELECT * FROM final
