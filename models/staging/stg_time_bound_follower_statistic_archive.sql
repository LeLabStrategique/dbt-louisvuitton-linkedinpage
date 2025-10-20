-- Fichier : models/staging/stg_time_bound_follower_statistic_archive.sql

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
        -- Extraction de la date
        DATE(day) AS day,
        -- Métriques de gain de followers (COALESCE pour éviter les NULL)
        COALESCE(follower_gains_organic_follower_gain, 0) AS organic_follower_gain,
        COALESCE(follower_gains_paid_follower_gain, 0) AS paid_follower_gain,
        
        organization_entity AS organization_id,
        
        _fivetran_id,
        _fivetran_synced,
        
        -- Dédoublonnage : sélectionne la ligne la plus récente pour chaque (day, organization_id)
        ROW_NUMBER() OVER (
            PARTITION BY DATE(day), organization_entity 
            ORDER BY _fivetran_synced DESC 
        ) AS rn
    FROM
        -- Utilisation de la macro {{ source() }}
        {{ source('linkedin_pages_normalized', 'time_bound_follower_statistic') }}
    
    -- La clause WHERE est conservée pour garantir l'intégrité de la clé de fusion
    WHERE day IS NOT NULL AND organization_entity IS NOT NULL
)

SELECT 
    day, 
    organic_follower_gain, 
    paid_follower_gain, 
    organization_id, 
    _fivetran_id, 
    _fivetran_synced
FROM source_data
WHERE rn = 1

-- Logique INCRÉMENTALE : dbt utilise cette condition pour ne traiter que les données pertinentes
{% if is_incremental() %}
AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}