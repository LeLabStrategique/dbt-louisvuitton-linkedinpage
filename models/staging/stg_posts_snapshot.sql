{{
    config(
        materialized='table', 
        schema='_STAGING',
        cluster_by=['post_id', 'day']
    )
}}

-- 1. Récupération des Attributs Descriptifs des Posts
WITH ugc_posts AS (
    SELECT
        id AS post_id,
        author,
        commentary,
        visibility,
        created_time,
        first_published_at,
        last_modified_time,
        lifecycle_state,
        _fivetran_synced AS ugc_post_synced,
        REGEXP_EXTRACT(author, r'organization:([0-9]+)') AS organization_id
    FROM 
        {{ source('linkedin_pages_normalized', 'ugc_post_history') }}
    WHERE 
        first_published_at IS NOT NULL
),

-- 2. Définition du Jour de Synchronisation (La date du snapshot)
post_content_sync AS (
    WITH ranked AS (
        SELECT
            post_id,
            type,
            DATE(_fivetran_synced) AS snapshot_day,
            ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY _fivetran_synced DESC) AS rn
        FROM 
            {{ source('linkedin_pages_normalized', 'post_content') }}
    )
    SELECT
        post_id,
        type,
        snapshot_day
    FROM ranked
    WHERE rn = 1
),

-- 3. Sélection du Dernier Snapshot de Métrique Dispo
latest_snapshots AS (
    WITH ranked AS (
        SELECT
            _share_entity_urn AS post_id,
            _organization_entity_urn,
            engagement,
            share_count,
            click_count,
            like_count,
            impression_count,
            comment_count,
            _fivetran_synced AS stats_synced_at,
            ROW_NUMBER() OVER(PARTITION BY _share_entity_urn ORDER BY _fivetran_synced DESC) AS rn
        FROM 
            {{ source('linkedin_pages_normalized', 'share_statistic') }}
    )
    SELECT
        post_id,
        _organization_entity_urn,
        engagement,
        share_count,
        click_count,
        like_count,
        impression_count,
        comment_count,
        stats_synced_at
    FROM ranked
    WHERE rn = 1
)

-- 4. Assemblage Final et Sélection des Colonnes
SELECT
    t2.snapshot_day AS day,
    t1.post_id,
    t2.type,
    t1.author,
    t1.commentary,
    t1.visibility,
    t1.created_time,
    t1.first_published_at,
    t1.last_modified_time,
    t1.lifecycle_state,
    t1.ugc_post_synced,
    COALESCE(t3.engagement, 0) AS engagement,
    COALESCE(t3.share_count, 0) AS share_count,
    COALESCE(t3.click_count, 0) AS click_count,
    COALESCE(t3.like_count, 0) AS like_count,
    COALESCE(t3.impression_count, 0) AS impression_count,
    COALESCE(t3.comment_count, 0) AS comment_count,
    t3.stats_synced_at AS _fivetran_synced,
    COALESCE(t1.organization_id, t3._organization_entity_urn) AS organization_id
FROM ugc_posts t1
INNER JOIN post_content_sync t2
    ON t1.post_id = t2.post_id
LEFT JOIN latest_snapshots t3
    ON t1.post_id = t3.post_id