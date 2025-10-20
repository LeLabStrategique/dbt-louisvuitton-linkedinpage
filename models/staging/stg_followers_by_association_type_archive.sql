{{
    config(
        materialized='incremental',
        unique_key=['organization_id', 'day', 'association_type_id'],
        cluster_by=['day']
    )
}}

WITH association_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      f.association_type AS association_type_id,
      f.association_type AS association_type_name,
      COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
      COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,
      
      ROW_NUMBER() OVER (
        PARTITION BY f._organization_entity_urn, DATE(f._fivetran_synced), f.association_type 
        ORDER BY f._fivetran_synced DESC
      ) AS rn
    
    FROM 
      {{ source('linkedin_pages_normalized', 'followers_by_association_type') }} f
)

SELECT
  day,
  association_type_id,
  association_type_name,
  follower_counts_organic_follower_count,
  follower_counts_paid_follower_count,
  _fivetran_id,
  _fivetran_synced,
  organization_id

FROM association_mapping

WHERE 
  rn = 1
  
  {% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
  {% endif %}