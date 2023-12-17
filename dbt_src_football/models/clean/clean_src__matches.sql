{{ config(materialized='table') }}

WITH final AS (

    SELECT
        id
        , home_team_score
        , away_team_score
        , date_time
        , DATE(date_time)                           AS match_date
        , stadium_name
        , home_team_id
        , home_team_name
        , home_team_kit_id
        , away_team_id
        , away_team_name
        , away_team_kit_id
        , home_team_coach_id
        , home_team_coach_preferred_name
        , away_team_coach_id
        , away_team_coach_preferred_name
        , competition_edition_id
        , competition_round_id
        , status
        , home_team_side
        , ball_trackable_object::INT                AS ball_trackable_object
        , pitch_length
        , pitch_width
    
    FROM {{ ref('stg_src__matches') }}
)

SELECT * FROM final
