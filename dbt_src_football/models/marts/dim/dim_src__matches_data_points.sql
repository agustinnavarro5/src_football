{{ config(materialized='table') }}

WITH final AS (

    SELECT
        match_id
        , date_time
        , point ->> 'trackable_object'  AS point_trackable_object
        , point ->> 'x'                 AS point_x
        , point ->> 'y'                 AS point_y
    FROM {{ ref('fct_src__matches_event_data') }}
    , JSONB_ARRAY_ELEMENTS(data) AS point
)

SELECT * FROM final