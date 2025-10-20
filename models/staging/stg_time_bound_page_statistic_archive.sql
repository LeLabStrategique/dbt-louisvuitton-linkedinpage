-- Fichier : models/staging/stg_time_bound_page_statistic_archive.sql

-- Configuration du modèle : utilisation du paramètre 'incremental' pour que dbt gère le MERGE (Upsert)
{{
    config(
        materialized='incremental',
        unique_key=['day', 'organization_id'],
        cluster_by=['day']
    )
}}

WITH source_data AS (
    SELECT
        -- Clés
        DATE(day) AS day,
        organization_entity AS organization_id,
        _fivetran_id,
        _fivetran_synced,
        
        -- Clicks (avec COALESCE)
        COALESCE(careers_page_promo_links_clicks, 0) AS careers_page_promo_links_clicks,
        COALESCE(careers_page_banner_promo_clicks, 0) AS careers_page_banner_promo_clicks,
        COALESCE(careers_page_jobs_clicks, 0) AS careers_page_jobs_clicks,
        COALESCE(careers_page_employees_clicks, 0) AS careers_page_employees_clicks,
        COALESCE(mobile_careers_page_promo_links_clicks, 0) AS mobile_careers_page_promo_links_clicks,
        COALESCE(mobile_careers_page_jobs_clicks, 0) AS mobile_careers_page_jobs_clicks,
        COALESCE(mobile_careers_page_employees_clicks, 0) AS mobile_careers_page_employees_clicks,
        
        -- Total Views (avec COALESCE)
        COALESCE(all_desktop_page_views, 0) AS all_desktop_page_views,
        COALESCE(all_desktop_unique_page_views, 0) AS all_desktop_unique_page_views,
        COALESCE(all_mobile_page_views, 0) AS all_mobile_page_views,
        COALESCE(all_mobile_unique_page_views, 0) AS all_mobile_unique_page_views,
        COALESCE(all_page_views, 0) AS all_page_views,
        COALESCE(all_unique_page_views, 0) AS all_unique_page_views,

        -- Segmented Views (avec COALESCE)
        COALESCE(about_page_views, 0) AS about_page_views,
        COALESCE(about_unique_page_views, 0) AS about_unique_page_views,
        COALESCE(careers_page_views, 0) AS careers_page_views,
        COALESCE(careers_unique_page_views, 0) AS careers_unique_page_views,
        COALESCE(products_page_views, 0) AS products_page_views,
        COALESCE(products_unique_page_views, 0) AS products_unique_page_views,
        COALESCE(jobs_page_views, 0) AS jobs_page_views,
        COALESCE(jobs_unique_page_views, 0) AS jobs_unique_page_views,
        COALESCE(people_page_views, 0) AS people_page_views,
        COALESCE(people_unique_page_views, 0) AS people_unique_page_views,
        COALESCE(overview_page_views, 0) AS overview_page_views,
        COALESCE(overview_unique_page_views, 0) AS overview_unique_page_views,
        COALESCE(life_at_page_views, 0) AS life_at_page_views,
        COALESCE(life_at_unique_page_views, 0) AS life_at_unique_page_views,
        COALESCE(insights_page_views, 0) AS insights_page_views,
        COALESCE(insights_unique_page_views, 0) AS insights_unique_page_views,
        COALESCE(mobile_careers_page_views, 0) AS mobile_careers_page_views,
        COALESCE(mobile_careers_unique_page_views, 0) AS mobile_careers_unique_page_views,
        COALESCE(mobile_overview_page_views, 0) AS mobile_overview_page_views,
        COALESCE(mobile_overview_unique_page_views, 0) AS mobile_overview_unique_page_views,
        COALESCE(mobile_jobs_page_views, 0) AS mobile_jobs_page_views,
        COALESCE(mobile_jobs_unique_page_views, 0) AS mobile_jobs_unique_page_views,
        COALESCE(mobile_life_at_page_views, 0) AS mobile_life_at_page_views,
        COALESCE(mobile_life_at_unique_page_views, 0) AS mobile_life_at_unique_page_views,
        COALESCE(mobile_insights_page_views, 0) AS mobile_insights_page_views,
        COALESCE(mobile_insights_unique_page_views, 0) AS mobile_insights_unique_page_views,
        COALESCE(mobile_products_page_views, 0) AS mobile_products_page_views,
        COALESCE(mobile_products_unique_page_views, 0) AS mobile_products_unique_page_views,
        COALESCE(mobile_about_page_views, 0) AS mobile_about_page_views,
        COALESCE(mobile_about_unique_page_views, 0) AS mobile_about_unique_page_views,
        COALESCE(mobile_people_page_views, 0) AS mobile_people_page_views,
        COALESCE(mobile_people_unique_page_views, 0) AS mobile_people_unique_page_views,
        
        -- Desktop Views supplémentaires
        COALESCE(desktop_insights_page_views, 0) AS desktop_insights_page_views,
        COALESCE(desktop_insights_unique_page_views, 0) AS desktop_insights_unique_page_views,
        COALESCE(desktop_careers_page_views, 0) AS desktop_careers_page_views,
        COALESCE(desktop_careers_unique_page_views, 0) AS desktop_careers_unique_page_views,
        COALESCE(desktop_life_at_page_views, 0) AS desktop_life_at_page_views,
        COALESCE(desktop_life_at_unique_page_views, 0) AS desktop_life_at_unique_page_views,
        
        -- Dédoublonnage : sélectionne la ligne la plus récente pour chaque (day, organization_id)
        ROW_NUMBER() OVER (
            PARTITION BY DATE(day), organization_entity 
            ORDER BY _fivetran_synced DESC 
        ) AS rn
        
    FROM
        -- Utilisation de la macro {{ source() }}
        {{ source('linkedin_pages_normalized', 'time_bound_page_statistic') }}
    
    -- La clause WHERE est conservée pour garantir l'intégrité de la clé de fusion
    WHERE day IS NOT NULL AND organization_entity IS NOT NULL
)

SELECT 
    -- Sélectionner toutes les colonnes SAUF 'rn'
    day, organization_id, 
    careers_page_promo_links_clicks, careers_page_banner_promo_clicks, careers_page_jobs_clicks, careers_page_employees_clicks, mobile_careers_page_promo_links_clicks, mobile_careers_page_jobs_clicks, mobile_careers_page_employees_clicks, 
    all_desktop_page_views, all_desktop_unique_page_views, all_mobile_page_views, all_mobile_unique_page_views, all_page_views, all_unique_page_views, 
    about_page_views, about_unique_page_views, careers_page_views, careers_unique_page_views, products_page_views, products_unique_page_views, jobs_page_views, jobs_unique_page_views, people_page_views, people_unique_page_views, overview_page_views, overview_unique_page_views, life_at_page_views, life_at_unique_page_views, insights_page_views, insights_unique_page_views, mobile_careers_page_views, mobile_careers_unique_page_views, mobile_overview_page_views, mobile_overview_unique_page_views, mobile_jobs_page_views, mobile_jobs_unique_page_views, mobile_life_at_page_views, mobile_life_at_unique_page_views, mobile_insights_page_views, mobile_insights_unique_page_views, mobile_products_page_views, mobile_products_unique_page_views, mobile_about_page_views, mobile_about_unique_page_views, mobile_people_page_views, mobile_people_unique_page_views, 
    desktop_insights_page_views, desktop_insights_unique_page_views, desktop_careers_page_views, desktop_careers_unique_page_views, desktop_life_at_page_views, desktop_life_at_unique_page_views, 
    _fivetran_id, _fivetran_synced
FROM source_data
WHERE rn = 1

-- Logique INCRÉMENTALE
{% if is_incremental() %}
AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}