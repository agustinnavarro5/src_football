{{ config(materialized='table') }}

WITH formatted_data AS (

    SELECT
        data::JSONB                                 AS data
        , possession::JSONB                         AS possession
        , frame::INT                                AS frame
        , image_corners_projection::JSONB           AS image_corners_projection
        , timestamp::VARCHAR                        AS date_time
        , period::INT                               AS period
        , filename::VARCHAR                         AS filename
    
    FROM {{ source('raw', 'matches_data') }}
)

, unfolded_data AS (

    SELECT
    data
    , possession ->> 'trackable_object'             AS possession_trackable_object
    , frame
    , image_corners_projection
    , date_time
    , period
    , SUBSTRING(filename FROM '^([0-9]+)')          AS match_id

    FROM formatted_data
)

, final AS (

    SELECT
        data
        , possession_trackable_object
        , frame
        , image_corners_projection
        , date_time
        , period
        , match_id::INT
    
    FROM unfolded_data
)

SELECT * FROM final
