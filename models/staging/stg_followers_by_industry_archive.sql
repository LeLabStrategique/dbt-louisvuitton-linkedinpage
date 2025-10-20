{{
    config(
        materialized='incremental',
        unique_key=['industry_id', 'day'],
        cluster_by=['day']
    )
}}

WITH industry_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      f.industry_id AS industry_id,
      COALESCE(i.name, 'Unknown') AS industry_name,
      COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
      COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,
      
      ROW_NUMBER() OVER (
        PARTITION BY f.industry_id, DATE(f._fivetran_synced) 
        ORDER BY f._fivetran_synced DESC
      ) AS rn
    
    FROM 
      {{ source('linkedin_pages_normalized', 'followers_by_industry') }} f
    LEFT JOIN 
      {{ source('linkedin_pages_normalized', 'industry') }} i
      ON f.industry_id = i.id
)

SELECT
  day,
  industry_id,
  industry_name,
  follower_counts_organic_follower_count,
  follower_counts_paid_follower_count,
  _fivetran_id,
  _fivetran_synced,
  organization_id

FROM industry_mapping

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}