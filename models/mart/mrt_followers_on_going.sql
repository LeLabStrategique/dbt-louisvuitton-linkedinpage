{{
    config(
        materialized='incremental',
        schema='_MART',
        unique_key=['day', 'dimension_id', 'dimension', 'organization_id'],
        cluster_by=['day', 'organization_id', 'dimension'],
        incremental_strategy='merge',
        on_schema_change='fail',
        merge_update_columns=[
            'dimension_name',
            'follower_counts_organic_follower_count',
            'follower_counts_paid_follower_count',
            '_fivetran_id',
            '_fivetran_synced',
            'geo_country_name',
            'continent',
            'Zone_LV',
            'Region_LV',
            'country_LV'
        ]
    )
}}

WITH source_data AS (
    SELECT
        day,
        dimension_id,
        dimension,
        dimension_name,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id,
        geo_country_name,
        continent,
        Zone_LV,
        Region_LV,
        country_LV
    FROM {{ ref('mrt_followers_daily') }}
)

SELECT
    day,
    dimension_id,
    dimension,
    dimension_name,
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    _fivetran_id,
    _fivetran_synced,
    organization_id,
    geo_country_name,
    continent,
    Zone_LV,
    Region_LV,
    country_LV
FROM source_data
{% if is_incremental() %}
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

MERGE INTO {{ this }} AS target
USING (
    SELECT
        day,
        dimension_id,
        dimension,
        dimension_name,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id,
        geo_country_name,
        continent,
        Zone_LV,
        Region_LV,
        country_LV
    FROM source_data
    WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
) AS source
ON target.day = source.day
   AND target.dimension_id = source.dimension_id
   AND target.dimension = source.dimension
   AND target.organization_id = source.organization_id
WHEN MATCHED AND source._fivetran_synced > target._fivetran_synced THEN
    UPDATE SET
        target.dimension_name = source.dimension_name,
        target.follower_counts_organic_follower_count = source.follower_counts_organic_follower_count,
        target.follower_counts_paid_follower_count = source.follower_counts_paid_follower_count,
        target._fivetran_id = source._fivetran_id,
        target._fivetran_synced = source._fivetran_synced,
        target.geo_country_name = source.geo_country_name,
        target.continent = source.continent,
        target.Zone_LV = source.Zone_LV,
        target.Region_LV = source.Region_LV,
        target.country_LV = source.country_LV
WHEN NOT MATCHED THEN
    INSERT (
        day,
        dimension_id,
        dimension,
        dimension_name,
        follower_counts_organic_follower_count,
        follower_counts_paid_follower_count,
        _fivetran_id,
        _fivetran_synced,
        organization_id,
        geo_country_name,
        continent,
        Zone_LV,
        Region_LV,
        country_LV
    )
    VALUES (
        source.day,
        source.dimension_id,
        source.dimension,
        source.dimension_name,
        source.follower_counts_organic_follower_count,
        source.follower_counts_paid_follower_count,
        source._fivetran_id,
        source._fivetran_synced,
        source.organization_id,
        source.geo_country_name,
        source.continent,
        source.Zone_LV,
        source.Region_LV,
        source.country_LV
    )
{% endif %}