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
        -- Correction: Renommer la colonne de sync ici pour l'utiliser plus tard
        t1.* EXCEPT(_fivetran_synced),
        t1._fivetran_synced AS ugc_post_synced, 
        REGEXP_EXTRACT(id, r'ugcPost:([0-9]+)$') AS extracted_ugc_post_id
    FROM 
        {{ source('linkedin_pages_normalized', 'ugc_post_history') }} t1 -- Ajout d'alias pour la clarté
),

-- 1. Jointure avec les statistiques de partage UGC
step_1_ugc_stats AS (
    SELECT 
        t1.*, 
        t2.share_statistic_id,
        t2.ugc_post_id
    FROM ugc_posts AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'ugc_post_share_statistic') }} AS t2
        ON t1.extracted_ugc_post_id = t2.ugc_post_id
),

-- 2. Jointure avec les statistiques de partage du Share_ID
step_2_share_stats AS (
    SELECT 
        t1.*, 
        t2.share_id
    FROM step_1_ugc_stats AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'share_share_statistic') }} AS t2
        ON t1.share_statistic_id = t2.share_statistic_id
),

-- 3. Jointure avec l'historique de partage
step_3_share_history AS (
    SELECT 
        t1.*, 
        t2.last_modified_time AS share_last_modified_time, 
        t2.created_time AS share_created_time,
        t2.commentary AS share_commentary,
        t2.lifecycle_state AS share_lifecycle_state,
        t2.first_published_at AS share_first_published_at,
        t2._fivetran_synced AS share_history_synced
    FROM step_2_share_stats AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'share_history') }} AS t2
        ON t1.share_id = t2.id
),

-- 3.5 CORRECTION : Dédoublonnage de l'historique du partage
step_3_deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY share_id 
            ORDER BY share_history_synced DESC 
        ) AS rn_history
    FROM step_3_share_history
    WHERE share_id IS NOT NULL 
),

-- 4. Jointure finale avec les statistiques d'engagement
master_table_raw AS (
    SELECT 
        t1.* EXCEPT(rn_history),
        t2.engagement,
        t2.share_count,
        t2.click_count,
        t2.like_count,
        t2.impression_count,
        t2.comment_count,
        t2._fivetran_synced AS share_statistic_synced,
        t2._organization_entity_urn
    FROM step_3_deduped AS t1
    LEFT JOIN {{ source('linkedin_pages_normalized', 'share_statistic') }} AS t2
        ON t1.share_statistic_id = t2._fivetran_id
    WHERE t1.share_statistic_id IS NOT NULL
      AND t1.rn_history = 1 
),

-- 5. Déduplication : on garde la ligne la plus récente par jour et par post_id
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id, DATE(share_statistic_synced)
            ORDER BY share_statistic_synced DESC
        ) AS rn
    FROM master_table_raw
    WHERE share_statistic_synced IS NOT NULL
)

-- 6. Sélection finale
SELECT 
    DATE(share_statistic_synced) AS day, 
    id AS post_id, 
    last_modified_time, 
    commentary, 
    created_time, 
    lifecycle_state, 
    first_published_at, 
    COALESCE(engagement, 0) AS engagement,
    COALESCE(share_count, 0) AS share_count,
    COALESCE(click_count, 0) AS click_count,
    COALESCE(like_count, 0) AS like_count,
    COALESCE(impression_count, 0) AS impression_count,
    COALESCE(comment_count, 0) AS comment_count,
    share_statistic_synced,
    ugc_post_synced, -- ⬅️ Nom de colonne corrigé
    ugc_post_id,
    _organization_entity_urn AS organization_id,
    share_statistic_synced IS NOT NULL AS has_live_stats
FROM deduplicated
WHERE rn = 1
{% if is_incremental() %}
  AND DATE(share_statistic_synced) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
{% endif %}