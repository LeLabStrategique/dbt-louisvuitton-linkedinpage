-- Fichier : models/staging/stg_ugc_post_statistics_archive.sql

{{
    config(
        materialized='incremental',
        unique_key=['post_id', 'day'],
        cluster_by=['day', 'organization_id']
    )
}}

-- 0. UGC Posts (Base de données des posts avec ID extrait)
WITH ugc_posts AS (
    SELECT
        *,
        -- Extrait l'ID numérique du post pour la jointure
        REGEXP_EXTRACT(id, r'ugcPost:([0-9]+)$') AS extracted_ugc_post_id
    FROM 
        {{ source('linkedin_pages_normalized', 'ugc_post_history') }}
),

-- 1. Jointure avec les statistiques de partage UGC
step_1_ugc_stats AS (
    SELECT 
        t1.* EXCEPT(extracted_ugc_post_id), 
        t2.share_statistic_id 
    FROM ugc_posts AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'ugc_post_share_statistic') }} AS t2
        ON t1.extracted_ugc_post_id = t2.ugc_post_id
),

-- 2. Jointure avec les statistiques de partage du Share_ID (via ugc_post_share_statistic)
step_2_share_stats AS (
    SELECT 
        t1.*, 
        t2.share_id
    FROM step_1_ugc_stats AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'share_share_statistic') }} AS t2
        ON t1.share_statistic_id = t2.share_statistic_id
),

-- 3. Jointure avec l'historique de partage pour les détails du commentaire/statut
step_3_share_history AS (
    SELECT 
        t1.*, 
        t2.last_modified_time AS share_last_modified_time, 
        t2.created_time AS share_created_time,
        t2.commentary AS share_commentary,
        t2.lifecycle_state AS share_lifecycle_state,
        t2.first_published_at AS share_first_published_at
    FROM step_2_share_stats AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'share_history') }} AS t2
        ON t1.share_id = t2.id
),

-- 4. Jointure finale pour obtenir les métriques d'engagement cumulées (Share_Statistic)
master_table_raw AS (
    SELECT 
        t1.*, 
        t2.engagement,
        t2.share_count,
        t2.click_count,
        t2.like_count,
        t2.impression_count,
        t2.comment_count,
        t2._fivetran_synced AS share_statistic_synced,
        t2._organization_entity_urn
    FROM step_3_share_history AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'share_statistic') }} AS t2
        -- Jointure sur le Fivetran ID de la table de stats, comme dans votre requête d'origine
        ON t1.share_statistic_id = t2._fivetran_id
    WHERE t1.share_statistic_id IS NOT NULL 
),

-- 5. Finalisation des colonnes et déduplication
deduplicate AS (
    SELECT
        -- CRÉATION DE LA CLÉ DAY : date du snapshot Fivetran des stats cumulées
        DATE(t.share_statistic_synced) AS day, 
        t.id AS post_id, 
        t.last_modified_time, 
        t.commentary, 
        t.created_time, 
        t.lifecycle_state, 
        t.first_published_at, 
        
        -- Métriques
        COALESCE(t.engagement, 0) AS engagement,
        COALESCE(t.share_count, 0) AS share_count,
        COALESCE(t.click_count, 0) AS click_count,
        COALESCE(t.like_count, 0) AS like_count,
        COALESCE(t.impression_count, 0) AS impression_count,
        COALESCE(t.comment_count, 0) AS comment_count,
        
        -- Métadonnées de synchronisation
        t.share_statistic_synced,
        t._fivetran_synced AS ugc_post_synced,
        
        t.ugc_post_id,
        t._organization_entity_urn AS organization_id,
        
        -- Indicateur de stats actives (statistiques cumulées sont présentes)
        t.share_statistic_synced IS NOT NULL AS has_live_stats,
        
        -- Logique de déduplication : prend le dernier snapshot par jour et par post
        ROW_NUMBER() OVER (
            PARTITION BY t.id, DATE(t.share_statistic_synced) 
            ORDER BY t.share_statistic_synced DESC
        ) AS rn
        
    FROM master_table_raw t
    WHERE t.share_statistic_synced IS NOT NULL -- Exclut les posts sans stats pour la clé 'day'
)

SELECT 
    day, post_id, last_modified_time, commentary, created_time, lifecycle_state, first_published_at, 
    engagement, share_count, click_count, like_count, impression_count, comment_count, 
    share_statistic_synced, ugc_post_synced, ugc_post_id, organization_id, has_live_stats
FROM deduplicate
WHERE rn = 1

-- Logique INCRÉMENTALE : ne traite que les données récentes pour le MERGE
{% if is_incremental() %}
AND DATE(day) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
{% endif %}