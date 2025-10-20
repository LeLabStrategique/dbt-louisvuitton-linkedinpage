{{
    config(
        materialized='incremental',
        unique_key=['function_id', 'day'],
        cluster_by=['day']
    )
}}

WITH function_mapping AS (
    SELECT
      f._fivetran_id,
      f._organization_entity_urn AS organization_id,
      f.function_id,
      COALESCE(fu.name, 'Unknown') AS function_name,
      COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
      COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
      f._fivetran_synced,
      DATE(f._fivetran_synced) AS day,
      
      ROW_NUMBER() OVER (
        PARTITION BY f.function_id, DATE(f._fivetran_synced)
        ORDER BY f._fivetran_synced DESC
      ) AS rn
    
    FROM 
      {{ source('linkedin_pages_normalized', 'followers_by_function') }} f
    LEFT JOIN 
      {{ source('linkedin_pages_normalized', 'function') }} fu
      ON f.function_id = fu.id
)

SELECT
  _fivetran_id,
  organization_id,
  function_id,
  function_name,
  follower_counts_organic_follower_count,
  follower_counts_paid_follower_count,
  _fivetran_synced,
  day

FROM function_mapping

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}