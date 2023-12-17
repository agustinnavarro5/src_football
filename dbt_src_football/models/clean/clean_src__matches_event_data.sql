{{ config(materialized='table') }}

WITH matches AS (

    SELECT
        id
        , match_date
    FROM {{ ref('clean_src__matches') }}
)

, final AS (

    SELECT
        med.data
        , med.possession_trackable_object
        , med.frame
        , med.image_corners_projection
        , CONCAT(
            ms.match_date
            , ' '
            , med.date_time
        )::TIMESTAMP                            AS date_time
        , med.period
        , med.match_id
    
    FROM {{ ref('stg_src__matches_event_data') }} AS med
    INNER JOIN matches AS ms ON med.match_id = ms.id
    WHERE
        data IS NOT NULL
)

SELECT * FROM final
