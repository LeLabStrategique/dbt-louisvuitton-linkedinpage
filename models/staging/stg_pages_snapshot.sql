{{
    config(
        materialized='table',
        schema='_STAGING',
        cluster_by=['day', 'organization_id', 'dimension']
    )
}}

WITH function_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.function_id AS STRING) AS dimension_id,
        'function' AS dimension,
        COALESCE(m.name, 'Unknown') AS dimension_name,
        COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views,
        COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views,
        COALESCE(f.all_page_views, 0) AS all_page_views,
        COALESCE(f.about_page_views, 0) AS about_page_views,
        COALESCE(f.careers_page_views, 0) AS careers_page_views,
        COALESCE(f.products_page_views, 0) AS products_page_views,
        COALESCE(f.jobs_page_views, 0) AS jobs_page_views,
        COALESCE(f.people_page_views, 0) AS people_page_views,
        COALESCE(f.overview_page_views, 0) AS overview_page_views,
        COALESCE(f.life_at_page_views, 0) AS life_at_page_views,
        COALESCE(f.insights_page_views, 0) AS insights_page_views,
        COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views,
        COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views,
        COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views,
        COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views,
        COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views,
        COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views,
        COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views,
        COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views,
        COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views,
        COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views,
        COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views,
        COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views,
        COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views,
        COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views,
        COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views,
        COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY function_id, DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'page_statistic_by_function') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'function') }} m
        ON f.function_id = m.id
    WHERE rn = 1
),

geo_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.geo_id AS STRING) AS dimension_id,
        'geo' AS dimension,
        COALESCE(m.value, 'Unknown') AS dimension_name,
        COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views,
        COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views,
        COALESCE(f.all_page_views, 0) AS all_page_views,
        COALESCE(f.about_page_views, 0) AS about_page_views,
        COALESCE(f.careers_page_views, 0) AS careers_page_views,
        COALESCE(f.products_page_views, 0) AS products_page_views,
        COALESCE(f.jobs_page_views, 0) AS jobs_page_views,
        COALESCE(f.people_page_views, 0) AS people_page_views,
        COALESCE(f.overview_page_views, 0) AS overview_page_views,
        COALESCE(f.life_at_page_views, 0) AS life_at_page_views,
        COALESCE(f.insights_page_views, 0) AS insights_page_views,
        COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views,
        COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views,
        COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views,
        COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views,
        COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views,
        COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views,
        COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views,
        COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views,
        COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views,
        COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views,
        COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views,
        COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views,
        COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views,
        COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views,
        COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views,
        COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(geo_id AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'page_statistic_by_geo') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'geo') }} m
        ON CAST(f.geo_id AS INT64) = m.id
    WHERE rn = 1
),

geo_country_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.geo_id AS STRING) AS dimension_id,
        'geo_country' AS dimension,
        COALESCE(m.value, 'Unknown') AS dimension_name,
        COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views,
        COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views,
        COALESCE(f.all_page_views, 0) AS all_page_views,
        COALESCE(f.about_page_views, 0) AS about_page_views,
        COALESCE(f.careers_page_views, 0) AS careers_page_views,
        COALESCE(f.products_page_views, 0) AS products_page_views,
        COALESCE(f.jobs_page_views, 0) AS jobs_page_views,
        COALESCE(f.people_page_views, 0) AS people_page_views,
        COALESCE(f.overview_page_views, 0) AS overview_page_views,
        COALESCE(f.life_at_page_views, 0) AS life_at_page_views,
        COALESCE(f.insights_page_views, 0) AS insights_page_views,
        COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views,
        COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views,
        COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views,
        COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views,
        COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views,
        COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views,
        COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views,
        COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views,
        COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views,
        COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views,
        COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views,
        COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views,
        COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views,
        COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views,
        COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views,
        COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(geo_id AS INT64), DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'page_statistic_by_geo_country') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'geo') }} m
        ON CAST(f.geo_id AS INT64) = m.id
    WHERE rn = 1
),

seniority_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.seniority_id AS STRING) AS dimension_id,
        'seniority' AS dimension,
        COALESCE(s.name, 'Unknown') AS dimension_name,
        COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views,
        COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views,
        COALESCE(f.all_page_views, 0) AS all_page_views,
        COALESCE(f.about_page_views, 0) AS about_page_views,
        COALESCE(f.careers_page_views, 0) AS careers_page_views,
        COALESCE(f.products_page_views, 0) AS products_page_views,
        COALESCE(f.jobs_page_views, 0) AS jobs_page_views,
        COALESCE(f.people_page_views, 0) AS people_page_views,
        COALESCE(f.overview_page_views, 0) AS overview_page_views,
        COALESCE(f.life_at_page_views, 0) AS life_at_page_views,
        COALESCE(f.insights_page_views, 0) AS insights_page_views,
        COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views,
        COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views,
        COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views,
        COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views,
        COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views,
        COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views,
        COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views,
        COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views,
        COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views,
        COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views,
        COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views,
        COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views,
        COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views,
        COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views,
        COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views,
        COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,
        f._fivetran_id,
        f._fivetran_synced,
        f._organization_entity_urn AS organization_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY seniority_id, DATE(_fivetran_synced)
                ORDER BY _fivetran_synced DESC
            ) AS rn
        FROM {{ source('linkedin_pages_normalized', 'page_statistic_by_seniority') }}
    ) f
    LEFT JOIN {{ source('linkedin_pages_normalized', 'seniority') }} s
        ON f.seniority_id = s.id
    WHERE rn = 1
),

staff_count_mapping AS (
    SELECT
        DATE(f._fivetran_synced) AS day,
        CAST(f.staff_count_range AS STRING) AS dimension_id,
        'staff_count_range' AS dimension,
        COALESCE(f.staff_count_range, 'Unknown') AS dimension_name,
        COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views,
        COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views,
        COALESCE(f.all_page_views, 0) AS all_page_views,
        COALESCE(f.about_page_views, 0) AS about_page_views,
        COALESCE(f.careers_page_views, 0) AS careers_page_views,
        COALESCE(f.products_page_views, 0) AS products_page_views,
        COALESCE(f.jobs_page_views, 0) AS jobs_page_views,
        COALESCE(f.people_page_views, 0) AS people_page_views,
        COALESCE(f.overview_page_views, 0) AS overview_page_views,
        COALESCE(f.life_at_page_views, 0) AS life_at_page_views,
        COALESCE(f.insights_page_views, 0) AS insights_page_views,
        COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views,
        COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views,
        COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views,
        COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views,
        COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views,
        COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views,
        COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views,
        COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views,
        COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views,
        COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views,
        COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views,
        COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views,
        COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views,
        COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views,
        COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views,
        COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,
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
        FROM {{ source('linkedin_pages_normalized', 'page_statistic_by_staff_count_range') }}
    ) f
    WHERE rn = 1
)

SELECT
    day,
    dimension_id,
    dimension,
    dimension_name,
    all_desktop_page_views,
    all_mobile_page_views,
    all_page_views,
    about_page_views,
    careers_page_views,
    products_page_views,
    jobs_page_views,
    people_page_views,
    overview_page_views,
    life_at_page_views,
    insights_page_views,
    mobile_careers_page_views,
    mobile_overview_page_views,
    mobile_jobs_page_views,
    mobile_life_at_page_views,
    mobile_insights_page_views,
    mobile_products_page_views,
    mobile_about_page_views,
    mobile_people_page_views,
    desktop_insights_page_views,
    desktop_careers_page_views,
    desktop_life_at_page_views,
    desktop_jobs_page_views,
    desktop_people_page_views,
    desktop_about_page_views,
    desktop_overview_page_views,
    desktop_products_page_views,
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
    all_desktop_page_views,
    all_mobile_page_views,
    all_page_views,
    about_page_views,
    careers_page_views,
    products_page_views,
    jobs_page_views,
    people_page_views,
    overview_page_views,
    life_at_page_views,
    insights_page_views,
    mobile_careers_page_views,
    mobile_overview_page_views,
    mobile_jobs_page_views,
    mobile_life_at_page_views,
    mobile_insights_page_views,
    mobile_products_page_views,
    mobile_about_page_views,
    mobile_people_page_views,
    desktop_insights_page_views,
    desktop_careers_page_views,
    desktop_life_at_page_views,
    desktop_jobs_page_views,
    desktop_people_page_views,
    desktop_about_page_views,
    desktop_overview_page_views,
    desktop_products_page_views,
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
    all_desktop_page_views,
    all_mobile_page_views,
    all_page_views,
    about_page_views,
    careers_page_views,
    products_page_views,
    jobs_page_views,
    people_page_views,
    overview_page_views,
    life_at_page_views,
    insights_page_views,
    mobile_careers_page_views,
    mobile_overview_page_views,
    mobile_jobs_page_views,
    mobile_life_at_page_views,
    mobile_insights_page_views,
    mobile_products_page_views,
    mobile_about_page_views,
    mobile_people_page_views,
    desktop_insights_page_views,
    desktop_careers_page_views,
    desktop_life_at_page_views,
    desktop_jobs_page_views,
    desktop_people_page_views,
    desktop_about_page_views,
    desktop_overview_page_views,
    desktop_products_page_views,
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
    all_desktop_page_views,
    all_mobile_page_views,
    all_page_views,
    about_page_views,
    careers_page_views,
    products_page_views,
    jobs_page_views,
    people_page_views,
    overview_page_views,
    life_at_page_views,
    insights_page_views,
    mobile_careers_page_views,
    mobile_overview_page_views,
    mobile_jobs_page_views,
    mobile_life_at_page_views,
    mobile_insights_page_views,
    mobile_products_page_views,
    mobile_about_page_views,
    mobile_people_page_views,
    desktop_insights_page_views,
    desktop_careers_page_views,
    desktop_life_at_page_views,
    desktop_jobs_page_views,
    desktop_people_page_views,
    desktop_about_page_views,
    desktop_overview_page_views,
    desktop_products_page_views,
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
    all_desktop_page_views,
    all_mobile_page_views,
    all_page_views,
    about_page_views,
    careers_page_views,
    products_page_views,
    jobs_page_views,
    people_page_views,
    overview_page_views,
    life_at_page_views,
    insights_page_views,
    mobile_careers_page_views,
    mobile_overview_page_views,
    mobile_jobs_page_views,
    mobile_life_at_page_views,
    mobile_insights_page_views,
    mobile_products_page_views,
    mobile_about_page_views,
    mobile_people_page_views,
    desktop_insights_page_views,
    desktop_careers_page_views,
    desktop_life_at_page_views,
    desktop_jobs_page_views,
    desktop_people_page_views,
    desktop_about_page_views,
    desktop_overview_page_views,
    desktop_products_page_views,
    _fivetran_id,
    _fivetran_synced,
    organization_id
FROM staff_count_mapping