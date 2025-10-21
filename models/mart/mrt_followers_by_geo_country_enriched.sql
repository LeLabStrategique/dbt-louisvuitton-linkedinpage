{{
    config(
        materialized='table',
        schema='linkedin_company_pages_MART',
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_followers_by_geo_country_archive') }}
),

regions AS (
    SELECT 
        country,
        country_LV,
        Region_LV,
        Zone_LV,
        continent
    FROM {{ source('linkedin_config', 'dim_country_region_LV') }}
),

final AS (
    SELECT
        b.day,
        b.geo_id,
        b.geo_name,
        b.organization_id,
        b.follower_counts_organic_follower_count,
        b.follower_counts_paid_follower_count,
        b._fivetran_id,
        b._fivetran_synced,
        COALESCE(r.country_LV, 'Unknown') AS country_LV,
        COALESCE(r.Region_LV, 'Unknown') AS Region_LV,
        COALESCE(r.Zone_LV, 'Unknown') AS Zone_LV,
        COALESCE(r.continent, 'Unknown') AS continent
    FROM base b
    LEFT JOIN regions r
        ON LOWER(b.geo_name) = LOWER(r.country)
)

SELECT * FROM final
