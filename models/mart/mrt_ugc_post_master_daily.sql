{{
    config(
        materialized='table',
        schema='linkedin_company_pages_MART',
    )
}}

WITH base_raw AS (
    -- Sélection de toutes les colonnes du modèle source.
    -- On utilise SELECT * pour s'assurer d'avoir les valeurs NULL avant COALESCE
    SELECT *
    FROM {{ ref('stg_ugc_post_statistics_archive') }}
),

base AS (
    SELECT
        p.day,
        p.post_id,
        p.organization_id,
        p.commentary,
        p.created_time,
        p.first_published_at,
        
        -- On sélectionne les métriques sans COALESCE pour vérifier leur nullité
        p.engagement,
        p.share_count,
        p.click_count,
        p.like_count,
        p.impression_count,
        p.comment_count,
        
        -- DÉFINITION DE has_live_stats : TRUE si au moins une métrique est non-NULL
        ((COALESCE(p.engagement, 0) + 
          COALESCE(p.share_count, 0) + 
          COALESCE(p.click_count, 0) + 
          COALESCE(p.like_count, 0) + 
          COALESCE(p.impression_count, 0) + 
          COALESCE(p.comment_count, 0)) > 0) AS has_live_stats
         
    FROM base_raw p
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
        ON REGEXP_REPLACE(LOWER(b.commentary), r'\s+', '') 
           LIKE CONCAT('%', REGEXP_REPLACE(LOWER(k.keyword), r'\s+', ''), '%')
),

final AS (
    SELECT
        *,
        COALESCE(matched_category, 'brand post') AS post_category,
        
        -- LOGIQUE DE NETTOYAGE ROBUSTE POUR post_title
        TRIM(
            REGEXP_REPLACE(
                COALESCE(
                    NULLIF(REGEXP_EXTRACT(commentary, r'^(.*?)[\\.\\,\\;\\:\\!\\?]'), ''),
                    SUBSTR(commentary, 1, 40)
                ), 
                r'[ \s]+', ' '
            )
        ) AS post_title,

        -- Âge du post en jours
        DATE_DIFF(day, DATE(first_published_at), DAY) AS post_age,
        
        -- Application du COALESCE aux métriques pour le résultat final
        COALESCE(engagement, 0) AS engagement_final,
        COALESCE(share_count, 0) AS share_count_final,
        COALESCE(click_count, 0) AS click_count_final,
        COALESCE(like_count, 0) AS like_count_final,
        COALESCE(impression_count, 0) AS impression_count_final,
        COALESCE(comment_count, 0) AS comment_count_final
        
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
    -- Utilisation des champs métriques finalisés
    engagement_final AS engagement,
    share_count_final AS share_count,
    click_count_final AS click_count,
    like_count_final AS like_count,
    impression_count_final AS impression_count,
    comment_count_final AS comment_count,
    
    -- Le boolean has_live_stats
    has_live_stats
FROM final