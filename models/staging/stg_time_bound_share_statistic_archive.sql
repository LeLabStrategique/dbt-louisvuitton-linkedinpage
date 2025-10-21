-- Fichier : models/staging/stg_time_bound_share_statistic_archive.sql

-- Configuration du modèle : utilisation du paramètre 'incremental' pour que dbt gère le MERGE (Upsert)
{{
    config(
        materialized='incremental',
        unique_key=['day', 'organization_id'],
        cluster_by=['day']
    )
}}

WITH source_data AS (
    SELECT
        -- Clés
        DATE(day) AS day,
        organization_entity AS organization_id,
        _fivetran_id,
        _fivetran_synced,
        
        -- Métriques (avec COALESCE pour garantir 0)
        COALESCE(engagement, 0) AS engagement,
        COALESCE(unique_impressions_count, 0) AS unique_impressions_count,
        COALESCE(share_count, 0) AS share_count,
        COALESCE(share_mentions_count, 0) AS share_mentions_count,
        COALESCE(click_count, 0) AS click_count,
        COALESCE(like_count, 0) AS like_count,
        COALESCE(impression_count, 0) AS impression_count,
        COALESCE(comment_count, 0) AS comment_count,
        COALESCE(comment_mentions_count, 0) AS comment_mentions_count,
        
        -- Dédoublonnage : sélectionne la ligne la plus récente
        ROW_NUMBER() OVER (
            PARTITION BY DATE(day), organization_entity 
            ORDER BY _fivetran_synced DESC 
        ) AS rn
        
    FROM
        {{ source('linkedin_pages_normalized', 'time_bound_share_statistic') }}
    
    -- La clause WHERE est conservée pour garantir l'intégrité de la clé de fusion
    WHERE day IS NOT NULL AND organization_entity IS NOT NULL
)

SELECT 
    -- Sélectionner toutes les colonnes SAUF 'rn'
    day, organization_id, engagement, unique_impressions_count, share_count, share_mentions_count, click_count, like_count, impression_count, comment_count, comment_mentions_count, _fivetran_id, _fivetran_synced
FROM source_data
WHERE rn = 1

-- Logique INCRÉMENTALE
{% if is_incremental() %}
AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}