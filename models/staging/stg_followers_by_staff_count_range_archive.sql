{{
    config(
        materialized='incremental',
        unique_key=['staff_count_range_id', 'day'],
        cluster_by=['day']
    )
}}

WITH staff_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      f.staff_count_range AS staff_count_range_id,
      f.staff_count_range AS staff_count_range_name,
      COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
      COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,
      
      ROW_NUMBER() OVER (
        PARTITION BY f.staff_count_range, DATE(f._fivetran_synced) 
        ORDER BY f._fivetran_synced DESC
      ) AS rn
      
    FROM 
      {{ source('linkedin_pages_normalized', 'followers_by_staff_count_range') }} f
)

SELECT
  day,
  staff_count_range_id,
  staff_count_range_name,
  follower_counts_organic_follower_count,
  follower_counts_paid_follower_count,
  _fivetran_id,
  _fivetran_synced,
  organization_id

FROM staff_mapping

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}