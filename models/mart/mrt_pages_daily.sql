{{
    config(
        materialized='table',
        schema='_MART',
        cluster_by=['day', 'organization_id', 'dimension']
    )
}}

WITH page_statistics_snapshot AS (
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
    FROM {{ ref('stg_pages_snapshot') }}
),

geo_country_enriched AS (
    SELECT
        p.day,
        p.dimension_id,
        p.dimension,
        p.dimension_name,
        p.all_desktop_page_views,
        p.all_mobile_page_views,
        p.all_page_views,
        p.about_page_views,
        p.careers_page_views,
        p.products_page_views,
        p.jobs_page_views,
        p.people_page_views,
        p.overview_page_views,
        p.life_at_page_views,
        p.insights_page_views,
        p.mobile_careers_page_views,
        p.mobile_overview_page_views,
        p.mobile_jobs_page_views,
        p.mobile_life_at_page_views,
        p.mobile_insights_page_views,
        p.mobile_products_page_views,
        p.mobile_about_page_views,
        p.mobile_people_page_views,
        p.desktop_insights_page_views,
        p.desktop_careers_page_views,
        p.desktop_life_at_page_views,
        p.desktop_jobs_page_views,
        p.desktop_people_page_views,
        p.desktop_about_page_views,
        p.desktop_overview_page_views,
        p.desktop_products_page_views,
        p._fivetran_id,
        p._fivetran_synced,
        p.organization_id,
        CASE 
            WHEN p.dimension = 'geo_country' THEN COALESCE(p.dimension_name, 'Unknown')
            ELSE NULL
        END AS geo_country_name,
        CASE 
            WHEN p.dimension = 'geo_country' THEN m.continent
            ELSE NULL
        END AS continent,
        CASE 
            WHEN p.dimension = 'geo_country' THEN m.Zone_LV
            ELSE NULL
        END AS Zone_LV,
        CASE 
            WHEN p.dimension = 'geo_country' THEN m.Region_LV
            ELSE NULL
        END AS Region_LV,
        CASE 
            WHEN p.dimension = 'geo_country' THEN m.country_LV
            ELSE NULL
        END AS country_LV
    FROM page_statistics_snapshot p
    LEFT JOIN {{ source('linkedin_config', 'dim_country_region_LV') }} m
        ON p.dimension_name = m.country
        AND p.dimension = 'geo_country'
)

SELECT
    day,
    dimension_id,
    dimension,
    dimension_name,
    geo_country_name,
    continent,
    Zone_LV,
    Region_LV,
    country_LV,
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
FROM geo_country_enriched