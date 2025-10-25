{{
    config(
        materialized='table',
        schema='_MART',
        cluster_by=['day', 'organization_id', 'dimension']
    )
}}

WITH followers_snapshot AS (
    SELECT
        day,
        dimension_id,
        dimension,
        dimension_name,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id
    FROM {{ ref('stg_followers_snapshot') }}
),

geo_country_enriched AS (
    SELECT
        f.day,
        f.dimension_id,
        f.dimension,
        f.dimension_name,
        f.follower_counts_organic_follower_count,
        f.follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f.organization_id,
        CASE 
            WHEN f.dimension = 'geo_country' THEN COALESCE(f.dimension_name, 'Unknown')
            ELSE NULL
        END AS geo_country_name,
        CASE 
            WHEN f.dimension = 'geo_country' THEN m.continent
            ELSE NULL
        END AS continent,
        CASE 
            WHEN f.dimension = 'geo_country' THEN m.Zone_LV
            ELSE NULL
        END AS Zone_LV,
        CASE 
            WHEN f.dimension = 'geo_country' THEN m.Region_LV
            ELSE NULL
        END AS Region_LV,
        CASE 
            WHEN f.dimension = 'geo_country' THEN m.country_LV
            ELSE NULL
        END AS country_LV
    FROM followers_snapshot f
    LEFT JOIN {{ source('linkedin_config', 'dim_country_region_LV') }} m
        ON f.dimension_name = m.country
        AND f.dimension = 'geo_country'
)

SELECT
    day,
    dimension_id,
    dimension,
    dimension_name,
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    _fivetran_id,
    _fivetran_synced,
    organization_id,
    geo_country_name,
    continent,
    Zone_LV,
    Region_LV,
    country_LV
FROM geo_country_enriched