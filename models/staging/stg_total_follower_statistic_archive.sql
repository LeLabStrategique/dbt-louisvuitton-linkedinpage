-- Fichier : models/staging/stg_total_follower_statistic_archive.sql

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
        _organization_entity_urn AS organization_id,
        _fivetran_id,
        _fivetran_synced,
        
        -- Métrique (avec COALESCE pour garantir 0)
        COALESCE(first_degree_size, 0) AS first_degree_size,
        
        -- Dédoublonnage : sélectionne la ligne la plus récente
        ROW_NUMBER() OVER (
            PARTITION BY DATE(day), _organization_entity_urn 
            ORDER BY _fivetran_synced DESC 
        ) AS rn
        
    FROM
        -- Utilisation de la macro {{ source() }}
        {{ source('linkedin_pages_normalized', 'total_follower_statistic') }}
    
    -- La clause WHERE est conservée pour garantir l'intégrité de la clé de fusion
    WHERE day IS NOT NULL AND _organization_entity_urn IS NOT NULL
)

SELECT 
    -- Sélectionner toutes les colonnes SAUF 'rn'
    day, 
    organization_id, 
    first_degree_size, 
    _fivetran_id, 
    _fivetran_synced
FROM source_data
WHERE rn = 1

-- Logique INCRÉMENTALE
{% if is_incremental() %}
AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}