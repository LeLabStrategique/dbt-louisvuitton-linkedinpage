-- Fichier : models/staging/stg_total_share_statistic_archive.sql

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
        -- Clé de granularité: DATE du dernier sync Fivetran
        DATE(_fivetran_synced) AS day, 
        _organization_entity_urn AS organization_id,
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
            PARTITION BY DATE(_fivetran_synced), _organization_entity_urn 
            ORDER BY _fivetran_synced DESC 
        ) AS rn
        
   FROM
        {{ source('linkedin_pages_normalized', 'total_share_statistic') }}

    
    -- La clause WHERE est conservée pour garantir l'intégrité de la clé de fusion
    WHERE _organization_entity_urn IS NOT NULL
)

SELECT 
    -- Sélectionner toutes les colonnes SAUF 'rn'
    day, engagement, unique_impressions_count, share_count, share_mentions_count, click_count, like_count, impression_count, comment_count, comment_mentions_count, _fivetran_id, _fivetran_synced, organization_id
FROM source_data
WHERE rn = 1

-- Logique INCRÉMENTALE
{% if is_incremental() %}
AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}