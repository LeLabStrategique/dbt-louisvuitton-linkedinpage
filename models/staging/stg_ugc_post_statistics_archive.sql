{{
    config(
        materialized='table',
        cluster_by=['post_id', 'day'],
        partition_by={
            "field": "day",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH ugc_posts AS (
    -- Étape 0: Sélection des colonnes de base et extraction de l'ID numérique
    SELECT
        id AS post_id,
        last_modified_time,
        author,
        commentary,
        visibility,
        created_time,
        lifecycle_state,
        first_published_at,
        _fivetran_synced AS ugc_post_synced,
        REGEXP_EXTRACT(id, r'ugcPost:([0-9]+)$') AS extracted_ugc_post_id
    FROM 
        {{ source('linkedin_pages_normalized', 'ugc_post_history') }}
),

join_share_metrics AS (
    -- Étape 1: Métriques Share Statistic (avec déduplication pour garantir le dernier snapshot par ID de métrique)
    SELECT
        _fivetran_id,
        engagement,
        share_count,
        click_count,
        like_count,
        impression_count,
        comment_count,
        _share_entity_urn,
        _organization_entity_urn,
        _fivetran_synced
    FROM 
        {{ source('linkedin_pages_normalized', 'share_statistic') }}
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY _fivetran_id 
            ORDER BY _fivetran_synced DESC
        ) = 1
)

-- Étape 2: Assemblage Final des données
SELECT
    DATE(t4._fivetran_synced) AS day,
    t1.post_id,
    t1.author,
    t1.commentary,
    t1.visibility,
    t1.created_time,
    t1.first_published_at,
    t1.last_modified_time,
    t1.lifecycle_state,
    t1.ugc_post_synced,
    
    -- Métriques
    COALESCE(t4.engagement, 0) AS engagement,
    COALESCE(t4.share_count, 0) AS share_count,
    COALESCE(t4.click_count, 0) AS click_count,
    COALESCE(t4.like_count, 0) AS like_count,
    COALESCE(t4.impression_count, 0) AS impression_count,
    COALESCE(t4.comment_count, 0) AS comment_count,
    
    -- Champ de synchronisation des stats
    t4._fivetran_synced,
    t4._organization_entity_urn as organization_id
    
FROM ugc_posts AS t1

-- Jointure : post_id (t1) = _share_entity_urn (t4)
LEFT JOIN join_share_metrics AS t4
    ON t1.post_id = t4._share_entity_urn