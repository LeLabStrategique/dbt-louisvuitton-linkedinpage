{{
    config(
        materialized='incremental',
        unique_key=['function_id', 'day'],
        cluster_by=['day']
    )
}}

WITH function_mapping AS (
    SELECT
      DATE(f._fivetran_synced) AS day,
      CAST(f.function_id AS INT64) AS function_id,
      COALESCE(fn.name, 'Unknown') AS function_name,
      COALESCE(f.follower_counts_organic_follower_count, 0) AS follower_counts_organic_follower_count,
      COALESCE(f.follower_counts_paid_follower_count, 0) AS follower_counts_paid_follower_count,
      f._fivetran_id,
      f._fivetran_synced,
      f._organization_entity_urn AS organization_id,

      ROW_NUMBER() OVER (
        PARTITION BY CAST(f.function_id AS INT64), DATE(f._fivetran_synced)
        ORDER BY f._fivetran_synced DESC
      ) AS rn

    FROM 
      {{ source('linkedin_pages_normalized', 'followers_by_function') }} f
    LEFT JOIN 
      {{ source('linkedin_pages_normalized', 'function') }} fn
      ON CAST(f.function_id AS INT64) = fn.id
)

SELECT
  day,
  function_id,
  function_name,
  follower_counts_organic_follower_count,
  follower_counts_paid_follower_count,
  _fivetran_id,
  _fivetran_synced,
  organization_id

FROM function_mapping
WHERE rn = 1

{% if is_incremental() %}
  AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
{% endif %}
