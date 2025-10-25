{{
    config(
        materialized='table',
        schema='_STAGING',
        cluster_by=['day', 'organization_id', 'dimension']
    )
}}

WITH association_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.association_type AS STRING) AS dimension_id,
        'association_type' AS dimension,
        f.association_type AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY _organization_entity_urn, association_type, DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_association_type') }}
    ) f
    WHERE rn = 1
),

function_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.function_id AS STRING) AS dimension_id,
        'function' AS dimension,
        COALESCE(fn.name, 'Unknown') AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(function_id AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_function') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'function') }} fn
        ON CAST(f.function_id AS INT64) = fn.id
    WHERE rn = 1
),

geo_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.geo AS STRING) AS dimension_id,
        'geo' AS dimension,
        COALESCE(g.value, 'Unknown') AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(geo AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_geo') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'geo') }} g
        ON CAST(f.geo AS INT64) = g.id
    WHERE rn = 1
),

geo_country_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.geo AS STRING) AS dimension_id,
        'geo_country' AS dimension,
        COALESCE(g.value, 'Unknown') AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(geo AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_geo_country') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'geo') }} g
        ON CAST(f.geo AS INT64) = g.id
    WHERE rn = 1
),

industry_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.industry_id AS STRING) AS dimension_id,
        'industry' AS dimension,
        COALESCE(fn.name, 'Unknown') AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(industry_id AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_industry') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'industry') }} fn
        ON CAST(f.industry_id AS INT64) = fn.id
    WHERE rn = 1
),

seniority_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.seniority_id AS STRING) AS dimension_id,
        'seniority' AS dimension,
        COALESCE(s.name, 'Unknown') AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(seniority_id AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_seniority') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'seniority') }} s
        ON CAST(f.seniority_id AS INT64) = s.id
    WHERE rn = 1
),

staff_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.staff_count_range AS STRING) AS dimension_id,
        'staff_count_range' AS dimension,
        f.staff_count_range AS dimension_name,
        COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
        COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY staff_count_range, DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'followers_by_staff_count_range') }}
    ) f
    WHERE rn = 1
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
    organization_id
FROM association_mapping
UNION ALL
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
FROM function_mapping
UNION ALL
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
FROM geo_mapping
UNION ALL
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
FROM geo_country_mapping
UNION ALL
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
FROM industry_mapping
UNION ALL
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
FROM seniority_mapping
UNION ALL
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
FROM staff_mapping