{{
    config(
        materialized='incremental',
        unique_key=['function_id', 'day'],
        cluster_by=['day']
    )
}}

WITH dimension_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      f.function_id AS function_id,
      COALESCE(m.name, 'Unknown') AS function_name,
      
      -- VUES DE PAGE (Metrics)
      COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views, COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views, COALESCE(f.all_page_views, 0) AS all_page_views, COALESCE(f.about_page_views, 0) AS about_page_views, COALESCE(f.careers_page_views, 0) AS careers_page_views, COALESCE(f.products_page_views, 0) AS products_page_views, COALESCE(f.jobs_page_views, 0) AS jobs_page_views, COALESCE(f.people_page_views, 0) AS people_page_views, COALESCE(f.overview_page_views, 0) AS overview_page_views, COALESCE(f.life_at_page_views, 0) AS life_at_page_views, COALESCE(f.insights_page_views, 0) AS insights_page_views, COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views, COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views, COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views, COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views, COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views, COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views, COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views, COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views, COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views, COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views, COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views, COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views, COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views, COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views, COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views, COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,

      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,
      
      ROW_NUMBER() OVER (
        PARTITION BY f.function_id, DATE(f._fivetran_synced)
        ORDER BY f._fivetran_synced DESC
      ) AS rn
      
    FROM 
      {{ source('linkedin_pages_normalized', 'page_statistic_by_function') }} f
    LEFT JOIN 
      {{ source('linkedin_pages_normalized', 'function') }} m
      ON f.function_id = m.id
)

SELECT 
  * EXCEPT(rn) 
  
FROM dimension_mapping

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}