{{
    config(
        materialized='table',
        schema='linkedin_company_pages_MART',
        cluster_by=['day', 'organization_id']
    )
}}

SELECT
    *
FROM 
    {{ ref('stg_total_follower_statistic_archive') }}