{{
    config(
        materialized='incremental',
        unique_key=['staff_count_range_name', 'day'],
        cluster_by=['day']
    )
}}

WITH raw_data AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      COALESCE(f.staff_count_range, 'Unknown') AS staff_count_range_id,
      COALESCE(f.staff_count_range, 'Unknown') AS staff_count_range_name,
      
      -- VUES DE PAGE (Metrics)
      COALESCE(f.all_desktop_page_views, 0) AS all_desktop_page_views, COALESCE(f.all_mobile_page_views, 0) AS all_mobile_page_views, COALESCE(f.all_page_views, 0) AS all_page_views, COALESCE(f.about_page_views, 0) AS about_page_views, COALESCE(f.careers_page_views, 0) AS careers_page_views, COALESCE(f.products_page_views, 0) AS products_page_views, COALESCE(f.jobs_page_views, 0) AS jobs_page_views, COALESCE(f.people_page_views, 0) AS people_page_views, COALESCE(f.overview_page_views, 0) AS overview_page_views, COALESCE(f.life_at_page_views, 0) AS life_at_page_views, COALESCE(f.insights_page_views, 0) AS insights_page_views, COALESCE(f.mobile_careers_page_views, 0) AS mobile_careers_page_views, COALESCE(f.mobile_overview_page_views, 0) AS mobile_overview_page_views, COALESCE(f.mobile_jobs_page_views, 0) AS mobile_jobs_page_views, COALESCE(f.mobile_life_at_page_views, 0) AS mobile_life_at_page_views, COALESCE(f.mobile_insights_page_views, 0) AS mobile_insights_page_views, COALESCE(f.mobile_products_page_views, 0) AS mobile_products_page_views, COALESCE(f.mobile_about_page_views, 0) AS mobile_about_page_views, COALESCE(f.mobile_people_page_views, 0) AS mobile_people_page_views, COALESCE(f.desktop_insights_page_views, 0) AS desktop_insights_page_views, COALESCE(f.desktop_careers_page_views, 0) AS desktop_careers_page_views, COALESCE(f.desktop_life_at_page_views, 0) AS desktop_life_at_page_views, COALESCE(f.desktop_jobs_page_views, 0) AS desktop_jobs_page_views, COALESCE(f.desktop_people_page_views, 0) AS desktop_people_page_views, COALESCE(f.desktop_about_page_views, 0) AS desktop_about_page_views, COALESCE(f.desktop_overview_page_views, 0) AS desktop_overview_page_views, COALESCE(f.desktop_products_page_views, 0) AS desktop_products_page_views,
      
      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,
      
      ROW_NUMBER() OVER (
        PARTITION BY f.staff_count_range, DATE(f._fivetran_synced)
        ORDER BY f._fivetran_synced DESC
      ) AS rn
      
    FROM 
      {{ source('linkedin_pages_normalized', 'page_statistic_by_staff_count_range') }} f
)

SELECT 
  * EXCEPT(rn) -- CONSERVE la syntaxe BigQuery (SELECT * EXCEPT)
  
FROM raw_data

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}