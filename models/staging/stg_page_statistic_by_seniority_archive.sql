-- Fichier : models/staging/stg_page_statistic_by_seniority_archive.sql

-- Configuration du modèle : utilisation du paramètre 'incremental' pour que dbt gère le MERGE (Upsert)
{{
    config(
        materialized='incremental',
        unique_key=['seniority_id', 'day'],
        cluster_by=['day']
    )
}}

WITH dimension_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      f.seniority_id AS seniority_id,
      -- Jointure avec la table 'seniority' pour obtenir le nom
      COALESCE(s.name, 'Unknown') AS seniority_name,
      
      -- TOUTES LES MÉTRIQUES DE VUES DE PAGE
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
      f._organization_entity_urn AS organization_id,

      -- Logique de déduplication : prend le dernier enregistrement par jour/seniorité (ROW_NUMBER)
      ROW_NUMBER() OVER (PARTITION BY f.seniority_id, DATE(f._fivetran_synced) ORDER BY f._fivetran_synced DESC) AS rn
      
    FROM 
      {{ source('linkedin_pages_normalized', 'page_statistic_by_seniority') }} f
    LEFT JOIN 
      {{ source('linkedin_pages_normalized', 'seniority') }} s
      ON f.seniority_id = s.id
  )
  
SELECT
  -- On sélectionne toutes les colonnes SAUF 'rn'
  day, seniority_id, seniority_name, all_desktop_page_views, all_mobile_page_views, all_page_views, about_page_views, careers_page_views, products_page_views, jobs_page_views, people_page_views, overview_page_views, life_at_page_views, insights_page_views, mobile_careers_page_views, mobile_overview_page_views, mobile_jobs_page_views, mobile_life_at_page_views, mobile_insights_page_views, mobile_products_page_views, mobile_about_page_views, mobile_people_page_views, desktop_insights_page_views, desktop_careers_page_views, desktop_life_at_page_views, desktop_jobs_page_views, desktop_people_page_views, desktop_about_page_views, desktop_overview_page_views, desktop_products_page_views, _fivetran_id, _fivetran_synced, organization_id

FROM dimension_mapping

WHERE 
  rn = 1
  
  -- Logique INCRÉMENTALE
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}