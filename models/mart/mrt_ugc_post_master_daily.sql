{{
    config(
        materialized='table',
        schema='linkedin_company_pages_MART',
    )
}}

WITH base AS (
    SELECT 
        p.day,
        p.post_id,
        p.organization_id,
        p.commentary,
        p.created_time,
        p.first_published_at,
        p.engagement,
        p.share_count,
        p.click_count,
        p.like_count,
        p.impression_count,
        p.comment_count,
        p.has_live_stats
    FROM {{ ref('stg_ugc_post_statistics_archive') }} p
),

keywords AS (
    SELECT 
        keyword,
        category
    FROM {{ source('linkedin_config', 'dim_post_category_keyword') }}
),

matched AS (
    SELECT 
        b.*,
        k.category AS matched_category
    FROM base b
    LEFT JOIN keywords k
        -- Logique pour la classification (normalisation générale des espaces)
        ON REGEXP_REPLACE(LOWER(b.commentary), r'\s+', '') 
           LIKE CONCAT('%', REGEXP_REPLACE(LOWER(k.keyword), r'\s+', ''), '%')
),

final AS (
    SELECT
        *,
        COALESCE(matched_category, 'brand post') AS post_category,
        
        -- Extraction du titre : jusqu'à la première ponctuation (. , ; : ! ?)
        REGEXP_EXTRACT(commentary, r'^(.*?)[\\.\\,\\;\\:\\!\\?]') AS raw_title,
        
        -- LOGIQUE DE NETTOYAGE ROBUSTE POUR post_title
        TRIM(
            REGEXP_REPLACE(
                -- 1. Récupère le titre extrait ou les 40 premiers caractères
                COALESCE(
                    NULLIF(REGEXP_EXTRACT(commentary, r'^(.*?)[\\.\\,\\;\\:\\!\\?]'), ''),
                    SUBSTR(commentary, 1, 40)
                ), 
                -- 2. REMPLACE L'ESPACE INSÉCABLE (U+00A0) par l'espace standard (U+0020)
                --    et remplace tous les autres caractères d'espacement multiples (sauts de ligne, tabs, etc.) par un seul espace standard.
                r'[ \s]+', ' '
            )
        ) AS post_title, -- Le post_title est maintenant nettoyé

        -- Âge du post en jours (différence entre le snapshot et la première publication)
        DATE_DIFF(day, DATE(first_published_at), DAY) AS post_age
    FROM matched
)

SELECT 
    day,
    post_id,
    organization_id,
    post_title,
    post_category,
    commentary,
    created_time,
    first_published_at,
    post_age,
    engagement,
    share_count,
    click_count,
    like_count,
    impression_count,
    comment_count,
    has_live_stats
FROM final