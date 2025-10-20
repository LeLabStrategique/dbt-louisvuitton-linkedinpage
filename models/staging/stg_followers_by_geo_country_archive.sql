{{
    config(
        materialized='incremental',
        unique_key=['geo_id', 'day'],
        cluster_by=['day']
    )
}}

WITH geo_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      CAST(f.geo AS INT64) AS geo_id,
      COALESCE(g.value, 'Unknown') AS geo_name, 
      COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
      COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,
      COALESCE(d.country_LV, 'Unknown') AS country_LV,
      COALESCE(d.Region_LV, 'Unknown') AS Region_LV,
      COALESCE(d.Zone_LV, 'Unknown') AS Zone_LV,
      COALESCE(d.continent, 'Unknown') AS continent,
      
      ROW_NUMBER() OVER (
        PARTITION BY CAST(f.geo AS INT64), DATE(f._fivetran_synced) 
        ORDER BY f._fivetran_synced DESC
      ) AS rn
      
    FROM 
      {{ source('linkedin_pages_normalized', 'followers_by_geo_country') }} f
    LEFT JOIN 
      {{ source('linkedin_pages_normalized', 'geo') }} g
      ON CAST(f.geo AS INT64) = g.id
    LEFT JOIN 
      {{ source('linkedin_config', 'dim_country_region_LV') }} d
      ON g.value = d.country 
)

SELECT
  day,
  geo_id,
  geo_name,
  follower_counts_organic_follower_count,
  follower_counts_paid_follower_count,
  _fivetran_id,
  _fivetran_synced,
  organization_id,
  country_LV,
  Region_LV,
  Zone_LV,
  continent

FROM geo_mapping

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}