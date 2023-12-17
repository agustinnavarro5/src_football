{{ config(materialized='table') }}

WITH formatted_data AS (

    SELECT
        id::INT                                     AS id
        , home_team_score::INT                      AS home_team_score
        , away_team_score::INT                      AS away_team_score
        , date_time::TIMESTAMP                      AS date_time
        , stadium::JSONB                            AS stadium
        , home_team::JSONB                          AS home_team
        , home_team_kit::JSONB                      AS home_team_kit
        , away_team::JSONB                          AS away_team
        , away_team_kit::JSONB                      AS away_team_kit
        , home_team_coach::JSONB                    AS home_team_coach
        , away_team_coach::JSONB                    AS away_team_coach
        , competition_edition::JSONB                AS competition_edition
        , competition_round::JSONB                  AS competition_round
        , referees::JSONB                           AS referees
        , players::JSONB                            AS players
        , status::VARCHAR                           AS status
        , home_team_side::JSONB                     AS home_team_side
        , ball::JSONB                               AS ball
        , pitch_length::INT                         AS pitch_length
        , pitch_width::INT                          AS pitch_width
    
    FROM {{ source('raw', 'matches_metadata') }}
)

, unfolded_data AS (

    SELECT
    id
    , home_team_score
    , away_team_score
    , date_time
    , stadium ->> 'name'                            AS stadium_name
    , home_team ->> 'id'                            AS home_team_id
    , home_team ->> 'name'                          AS home_team_name
    , home_team_kit ->> 'id'                        AS home_team_kit_id
    , away_team ->> 'id'                            AS away_team_id
    , away_team ->> 'name'                          AS away_team_name
    , away_team_kit ->> 'id'                        AS away_team_kit_id
    , home_team_coach ->> 'id'                      AS home_team_coach_id
    , home_team_coach ->> 'first_name'              AS home_team_coach_first_name
    , home_team_coach ->> 'last_name'               AS home_team_coach_last_name
    , away_team_coach ->> 'id'                      AS away_team_coach_id
    , away_team_coach ->> 'first_name'              AS away_team_coach_first_name
    , away_team_coach ->> 'last_name'               AS away_team_coach_last_name
    , competition_edition ->> 'id'                  AS competition_edition_id
    , competition_round ->> 'id'                    AS competition_round_id
    , referees
    , players
    , status
    , home_team_side
    , ball ->> 'trackable_object'                   AS ball_trackable_object
    , pitch_length
    , pitch_width

    FROM formatted_data
)

, final AS (

    SELECT
        id
        , home_team_score
        , away_team_score
        , date_time
        , stadium_name
        , home_team_id::INT                         AS home_team_id
        , home_team_name::VARCHAR                   AS home_team_name
        , home_team_kit_id::INT                     AS home_team_kit_id
        , away_team_id::INT                         AS away_team_id
        , away_team_name::VARCHAR                   AS away_team_name
        , away_team_kit_id::INT                     AS away_team_kit_id
        , home_team_coach_id::INT                   AS home_team_coach_id
        , CONCAT(
            home_team_coach_last_name
          , ', '
          , home_team_coach_first_name  
        )::VARCHAR                                  AS home_team_coach_preferred_name
        , away_team_coach_id::INT                   AS away_team_coach_id
        , CONCAT(
            away_team_coach_last_name
          , ', '
          , away_team_coach_first_name  
        )::VARCHAR                                  AS away_team_coach_preferred_name
        , competition_edition_id::INT               AS competition_edition_id
        , competition_round_id::INT                 AS competition_round_id
        , referees
        , players
        , status
        , home_team_side
        , ball_trackable_object::INT                AS ball_trackable_object
        , pitch_length
        , pitch_width
    
    FROM unfolded_data
)

SELECT * FROM final
