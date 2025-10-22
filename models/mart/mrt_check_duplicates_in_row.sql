-- Objectif : Détecter les doublons parfaits de ligne pour une même clé (post_id, day)

-- 1. Hacher chaque ligne du Mart pour obtenir une signature unique
WITH HashedTable AS (
    SELECT
        t.*,
        -- Utilise SHA256 sur la représentation JSON de la ligne entière (t) pour générer une signature unique
        SHA256(TO_JSON_STRING(t)) AS row_hash
    FROM 
        {{ ref('mrt_ugc_post_master_daily') }} AS t
),

-- 2. Vérifier si plusieurs signatures existent pour la même clé (post_id, day)
check_duplicates AS (
    SELECT
        post_id,
        day,
        COUNT(DISTINCT row_hash) AS distinct_rows 
    FROM
        HashedTable
    GROUP BY
        1, 2
    HAVING
        -- Si cette condition est > 1, cela signifie que plus d'une ligne unique 
        -- a été insérée pour la même combinaison post/jour
        COUNT(DISTINCT row_hash) > 1
)

SELECT * FROM check_duplicates