{{
    config(
        materialized='table', 
        schema='_MART',
        cluster_by=['day', 'organization_id', 'post_category']
    )
}}

-- 1. Récupération des données du snapshot du jour (stg_posts_snapshot)
WITH daily_snapshot AS (
    SELECT 
        day,
        post_id,
        organization_id,
        type,
        author,
        commentary,
        visibility,
        created_time,
        first_published_at,
        last_modified_time,
        lifecycle_state,
        ugc_post_synced,
        _fivetran_synced,
        engagement,
        share_count,
        click_count,
        like_count,
        impression_count,
        comment_count,
        CASE 
            WHEN COALESCE(engagement, 0) + COALESCE(share_count, 0) + COALESCE(click_count, 0) + 
                 COALESCE(like_count, 0) + COALESCE(impression_count, 0) + COALESCE(comment_count, 0) > 0 
            THEN TRUE 
            ELSE FALSE 
        END AS has_stats,
        DATE_DIFF(day, DATE(first_published_at), DAY) AS post_age
    FROM {{ ref('stg_posts_snapshot') }}
),

keywords AS (
    -- 2. Dictionnaire des mots-clés (pour identifier les posts "HR")
    SELECT DISTINCT
        keyword
    FROM {{ source('linkedin_config', 'dim_post_category_keyword') }}
),

classified AS (
    -- 3. Ajout de la classification (post_category et post_title)
    SELECT 
        p.*,
        CASE 
            WHEN COUNT(k.keyword) > 0 THEN 'HR post'
            ELSE 'Brand post'
        END AS post_category,
        TRIM(
            REGEXP_REPLACE(
                COALESCE(
                    NULLIF(REGEXP_EXTRACT(COALESCE(p.commentary, ''), r'^([^.!?;,:]+)[.!?;:,]'), ''),
                    SUBSTR(COALESCE(p.commentary, ''), 1, 40)
                ), 
                r'[ \s]+', ' '
            )
        ) AS post_title
    FROM daily_snapshot p
    LEFT JOIN keywords k 
        ON REGEXP_REPLACE(LOWER(p.commentary), r'[ \s]+', '') 
           LIKE CONCAT('%', REGEXP_REPLACE(LOWER(k.keyword), r'[ \s]+', ''), '%')
    GROUP BY 
        p.day, p.post_id, p.organization_id, p.type, p.author, p.commentary, 
        p.visibility, p.created_time, p.first_published_at, p.last_modified_time, 
        p.lifecycle_state, p.ugc_post_synced, p._fivetran_synced, p.engagement, 
        p.share_count, p.click_count, p.like_count, p.impression_count, 
        p.comment_count, p.has_stats, p.post_age
)

-- 4. Sélection finale : Tous les champs du snapshot + les champs dérivés
SELECT 
    day,
    post_id,
    organization_id,
    post_title,
    post_category,
    post_age,
    type,
    author,
    commentary,
    visibility,
    created_time,
    first_published_at,
    last_modified_time,
    lifecycle_state,
    ugc_post_synced,
    _fivetran_synced,
    engagement,
    share_count,
    click_count,
    like_count,
    impression_count,
    comment_count,
    has_stats
FROM classified