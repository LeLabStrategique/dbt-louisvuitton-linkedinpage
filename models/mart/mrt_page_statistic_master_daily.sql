{{ config(
    materialized='table',
    schema='linkedin_company_pages_MART',
    cluster_by=['day', 'organization_id'],
    partition_by={'field': 'day', 'data_type': 'date'}
) }}

-- =======================================
-- 1️⃣ page stats by function
-- =======================================
 WITH page_function AS (
    SELECT
        day,
        'function' AS dimension_name,
        function_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        all_desktop_page_views, all_mobile_page_views, all_page_views,
        about_page_views, careers_page_views, products_page_views,
        jobs_page_views, people_page_views, overview_page_views,
        life_at_page_views, insights_page_views,
        mobile_careers_page_views, mobile_overview_page_views,
        mobile_jobs_page_views, mobile_life_at_page_views,
        mobile_insights_page_views, mobile_products_page_views,
        mobile_about_page_views, mobile_people_page_views,
        desktop_insights_page_views, desktop_careers_page_views,
        desktop_life_at_page_views, desktop_jobs_page_views,
        desktop_people_page_views, desktop_about_page_views,
        desktop_overview_page_views, desktop_products_page_views,
        _fivetran_id, _fivetran_synced, organization_id
    FROM {{ ref('stg_page_statistic_by_function_archive') }}
),


-- =======================================
-- 3️⃣ page stats by seniority
-- =======================================
page_seniority AS (
    SELECT
        day,
        'seniority' AS dimension_name,
        seniority_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        all_desktop_page_views, all_mobile_page_views, all_page_views,
        about_page_views, careers_page_views, products_page_views,
        jobs_page_views, people_page_views, overview_page_views,
        life_at_page_views, insights_page_views,
        mobile_careers_page_views, mobile_overview_page_views,
        mobile_jobs_page_views, mobile_life_at_page_views,
        mobile_insights_page_views, mobile_products_page_views,
        mobile_about_page_views, mobile_people_page_views,
        desktop_insights_page_views, desktop_careers_page_views,
        desktop_life_at_page_views, desktop_jobs_page_views,
        desktop_people_page_views, desktop_about_page_views,
        desktop_overview_page_views, desktop_products_page_views,
        _fivetran_id, _fivetran_synced, organization_id
    FROM {{ ref('stg_page_statistic_by_seniority_archive') }}
),

-- =======================================
-- 4️⃣ page stats by staff_count_range
-- =======================================
page_staff_count AS (
    SELECT
        day,
        'staff_count_range' AS dimension_name,
        staff_count_range_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        all_desktop_page_views, all_mobile_page_views, all_page_views,
        about_page_views, careers_page_views, products_page_views,
        jobs_page_views, people_page_views, overview_page_views,
        life_at_page_views, insights_page_views,
        mobile_careers_page_views, mobile_overview_page_views,
        mobile_jobs_page_views, mobile_life_at_page_views,
        mobile_insights_page_views, mobile_products_page_views,
        mobile_about_page_views, mobile_people_page_views,
        desktop_insights_page_views, desktop_careers_page_views,
        desktop_life_at_page_views, desktop_jobs_page_views,
        desktop_people_page_views, desktop_about_page_views,
        desktop_overview_page_views, desktop_products_page_views,
        _fivetran_id, _fivetran_synced, organization_id
    FROM {{ ref('stg_page_statistic_by_staff_count_range_archive') }}
),

-- =======================================
-- 5️⃣ page stats by geo_country (enrichies)
-- =======================================
page_geo_country AS (
    SELECT
        f.day,
        'geo_country' AS dimension_name,
        f.geo_country_name AS dimension_value,
        COALESCE(m.country_LV, 'Unknown') AS country,
        COALESCE(m.Region_LV, 'Unknown') AS region,
        COALESCE(m.Zone_LV, 'Unknown') AS zone,
        f.all_desktop_page_views, f.all_mobile_page_views, f.all_page_views,
        f.about_page_views, f.careers_page_views, f.products_page_views,
        f.jobs_page_views, f.people_page_views, f.overview_page_views,
        f.life_at_page_views, f.insights_page_views,
        f.mobile_careers_page_views, f.mobile_overview_page_views,
        f.mobile_jobs_page_views, f.mobile_life_at_page_views,
        f.mobile_insights_page_views, f.mobile_products_page_views,
        f.mobile_about_page_views, f.mobile_people_page_views,
        f.desktop_insights_page_views, f.desktop_careers_page_views,
        f.desktop_life_at_page_views, f.desktop_jobs_page_views,
        f.desktop_people_page_views, f.desktop_about_page_views,
        f.desktop_overview_page_views, f.desktop_products_page_views,
        f._fivetran_id, f._fivetran_synced, f.organization_id
    FROM {{ ref('stg_page_statistic_by_geo_country_archive') }} f
    LEFT JOIN {{ source('linkedin_config', 'dim_country_region_LV') }} m
        ON f.geo_country_name = m.country
),

-- =======================================
-- 6️⃣ page stats by geo
-- =======================================
page_geo AS (
    SELECT
        day,
        'geo' AS dimension_name,
        geo_name AS dimension_value,
        CAST(NULL AS STRING) AS country,
        CAST(NULL AS STRING) AS region,
        CAST(NULL AS STRING) AS zone,
        all_desktop_page_views, all_mobile_page_views, all_page_views,
        about_page_views, careers_page_views, products_page_views,
        jobs_page_views, people_page_views, overview_page_views,
        life_at_page_views, insights_page_views,
        mobile_careers_page_views, mobile_overview_page_views,
        mobile_jobs_page_views, mobile_life_at_page_views,
        mobile_insights_page_views, mobile_products_page_views,
        mobile_about_page_views, mobile_people_page_views,
        desktop_insights_page_views, desktop_careers_page_views,
        desktop_life_at_page_views, desktop_jobs_page_views,
        desktop_people_page_views, desktop_about_page_views,
        desktop_overview_page_views, desktop_products_page_views,
        _fivetran_id, _fivetran_synced, organization_id
    FROM {{ ref('stg_page_statistic_by_geo_archive') }}
)

-- =======================================
-- 7️⃣ UNION GLOBAL
-- =======================================
SELECT * FROM page_function
/*UNION ALL
SELECT * FROM page_industry*/
UNION ALL
SELECT * FROM page_seniority
UNION ALL
SELECT * FROM page_staff_count
UNION ALL
SELECT * FROM page_geo_country
UNION ALL
SELECT * FROM page_geo
