{{ config(
    materialized='table',
    schema='linkedin_company_pages_MART',
    cluster_by=['day', 'organization_id'],
    partition_by={'field': 'day', 'data_type': 'date'}
) }}

WITH followers_function AS (
    SELECT
        day,
        'function' AS dimension_name,
        function_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id
    FROM {{ ref('stg_followers_by_function_archive') }}
),

followers_industry AS (
    SELECT
        day,
        'industry' AS dimension_name,
        industry_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id
    FROM {{ ref('stg_followers_by_industry_archive') }}
),

followers_seniority AS (
    SELECT
        day,
        'seniority' AS dimension_name,
        seniority_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id
    FROM {{ ref('stg_followers_by_seniority_archive') }}
),

followers_staff_count AS (
    SELECT
        day,
        'staff_count_range' AS dimension_name,
        staff_count_range_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id
    FROM {{ ref('stg_followers_by_staff_count_range_archive') }}
),

followers_geo_country AS (
    SELECT
        f.day,
        'geo_country' AS dimension_name,
        f.geo_name AS dimension_value,
        COALESCE(m.country_LV, 'Unknown') AS country,
        COALESCE(m.Region_LV, 'Unknown') AS region,
        COALESCE(m.Zone_LV, 'Unknown') AS zone,
        f.follower_counts_organic_follower_count,
        f.follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f.organization_id
    FROM {{ ref('stg_followers_by_geo_country_archive') }} f
    LEFT JOIN {{ source('linkedin_config', 'dim_country_region_LV') }} m
        ON f.geo_name = m.country
),

followers_geo AS (
    SELECT
        day,
        'geo' AS dimension_name,
        geo_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id
    FROM {{ ref('stg_followers_by_geo_archive') }}
)

SELECT * FROM followers_function
UNION ALL
SELECT * FROM followers_industry
UNION ALL
SELECT * FROM followers_seniority
UNION ALL
SELECT * FROM followers_staff_count
UNION ALL
SELECT * FROM followers_geo_country
UNION ALL
SELECT * FROM followers_geo
