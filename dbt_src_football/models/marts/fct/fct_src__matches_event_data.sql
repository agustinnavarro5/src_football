{{ config(materialized='table') }}

WITH players AS (

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
        med.data
        , players.player_id
        , players.player_team_id
        , players.player_name
        , players.player_trackable_object
        , med.frame
        , med.image_corners_projection
        , med.date_time
        , med.period
        , med.match_id
    FROM {{ ref('clean_src__matches_event_data') }} AS med
    LEFT JOIN players           ON players.match_id = med.match_id
                                AND players.player_trackable_object = med.possession_trackable_object
    -- We want just these timestamp where a possession trackable object was detected
    WHERE
        players.player_trackable_object IS NOT NULL
)

SELECT * FROM final
