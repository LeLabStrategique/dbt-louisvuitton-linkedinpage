{{
    config(
        materialized='incremental',
        schema='_MART',
        unique_key=['post_id', 'day'],
        cluster_by=['day', 'organization_id', 'post_category'],
        incremental_strategy='merge',
        on_schema_change='fail',
        merge_update_columns=[
            'post_id', 'organization_id', 'type', 'author', 'commentary', 'visibility', 
            'created_time', 'first_published_at', 'last_modified_time', 'lifecycle_state', 
            'ugc_post_synced', '_fivetran_synced', 'engagement', 'share_count', 'click_count', 
            'like_count', 'impression_count', 'comment_count', 'has_stats', 
            'post_category', 'post_title', 'post_age'
        ]
    )
}}

WITH source_data AS (
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
        has_stats,
        post_category,
        post_title,
        post_age
    FROM {{ ref('mrt_posts_daily') }}
)

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
    has_stats,
    post_category,
    post_title,
    post_age
FROM source_data
{% if is_incremental() %}
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)

MERGE INTO {{ this }} AS target
USING (
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
        has_stats,
        post_category,
        post_title,
        post_age
    FROM source_data
    WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
) AS source
ON target.post_id = source.post_id AND target.day = source.day
WHEN MATCHED AND source._fivetran_synced > target._fivetran_synced THEN
    UPDATE SET
        target.organization_id = source.organization_id,
        target.type = source.type,
        target.author = source.author,
        target.commentary = source.commentary,
        target.visibility = source.visibility,
        target.created_time = source.created_time,
        target.first_published_at = source.first_published_at,
        target.last_modified_time = source.last_modified_time,
        target.lifecycle_state = source.lifecycle_state,
        target.ugc_post_synced = source.ugc_post_synced,
        target._fivetran_synced = source._fivetran_synced,
        target.engagement = source.engagement,
        target.share_count = source.share_count,
        target.click_count = source.click_count,
        target.like_count = source.like_count,
        target.impression_count = source.impression_count,
        target.comment_count = source.comment_count,
        target.has_stats = source.has_stats,
        target.post_category = source.post_category,
        target.post_title = source.post_title,
        target.post_age = source.post_age
WHEN NOT MATCHED THEN
    INSERT (
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
        has_stats,
        post_category,
        post_title,
        post_age
    )
    VALUES (
        source.day,
        source.post_id,
        source.organization_id,
        source.type,
        source.author,
        source.commentary,
        source.visibility,
        source.created_time,
        source.first_published_at,
        source.last_modified_time,
        source.lifecycle_state,
        source.ugc_post_synced,
        source._fivetran_synced,
        source.engagement,
        source.share_count,
        source.click_count,
        source.like_count,
        source.impression_count,
        source.comment_count,
        source.has_stats,
        source.post_category,
        source.post_title,
        source.post_age
    )
{% endif %}