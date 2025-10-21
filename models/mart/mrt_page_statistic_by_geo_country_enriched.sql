{{
    config(
        materialized='table',
        schema='linkedin_company_pages_MART',
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_page_statistic_by_geo_country_archive') }}
),

regions AS (
    SELECT
        country,
        country_LV,
        Region_LV,
        Zone_LV,
        continent
    FROM {{ source('linkedin_config', 'dim_country_region_LV') }}
)

SELECT
    b.day,
    b.geo_country_id,
    b.geo_country_name,
    b.organization_id,
    b.all_desktop_page_views,
    b.all_mobile_page_views,
    b.all_page_views,
    b.about_page_views,
    b.careers_page_views,
    b.products_page_views,
    b.jobs_page_views,
    b.people_page_views,
    b.overview_page_views,
    b.life_at_page_views,
    b.insights_page_views,
    b.mobile_careers_page_views,
    b.mobile_overview_page_views,
    b.mobile_jobs_page_views,
    b.mobile_life_at_page_views,
    b.mobile_insights_page_views,
    b.mobile_products_page_views,
    b.mobile_about_page_views,
    b.mobile_people_page_views,
    b.desktop_insights_page_views,
    b.desktop_careers_page_views,
    b.desktop_life_at_page_views,
    b.desktop_jobs_page_views,
    b.desktop_people_page_views,
    b.desktop_about_page_views,
    b.desktop_overview_page_views,
    b.desktop_products_page_views,
    b._fivetran_id,
    b._fivetran_synced,
    COALESCE(r.country_LV, 'Unknown') AS country_LV,
    COALESCE(r.Region_LV, 'Unknown') AS Region_LV,
    COALESCE(r.Zone_LV, 'Unknown') AS Zone_LV,
    COALESCE(r.continent, 'Unknown') AS continent
FROM base b
LEFT JOIN regions r
    ON LOWER(b.geo_country_name) = LOWER(r.country)
