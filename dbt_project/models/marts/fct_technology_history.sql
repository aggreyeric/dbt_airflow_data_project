{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['snapshot_date', 'technology_name'],
    on_schema_change='sync_all_columns',
    schema='marts'
  )
}}

WITH current_metrics AS (
    SELECT * FROM {{ ref('fct_technology_metrics') }}
),

{% if is_incremental() %}

previous_day_metrics AS (
    SELECT 
        sub.technology_name,
        sub.github_stars,
        sub.github_forks,
        sub.pypi_downloads_daily,
        sub.open_issues
    FROM (
        SELECT
            hist.technology_name,
            hist.github_stars,
            hist.github_forks,
            hist.pypi_downloads_daily,
            hist.open_issues,
            ROW_NUMBER() OVER (PARTITION BY hist.technology_name ORDER BY hist.snapshot_date DESC) as rn
        FROM {{ this }} AS hist
    ) AS sub
    WHERE sub.rn = 1
),

final_model AS (
    SELECT
        cm.technology_name,
        cm.github_repo,
        cm.pypi_package,
        cm.github_stars,
        cm.github_forks,
        cm.github_watchers,
        cm.pypi_downloads_daily,
        cm.pypi_downloads_weekly,
        cm.pypi_downloads_monthly,
        cm.open_issues,
        cm.contributors_count,
        cm.github_releases,
        cm.pypi_release_count,
        cm.fork_to_star_ratio,
        cm.stars_per_contributor,
        cm.popularity_tier,
        cm.usage_tier,
        cm.github_created_at,
        cm.github_updated_at,
        cm.latest_release_published_at,
        cm.latest_release_upload_time,
        cm.snapshot_date,
        cm.last_updated_at,
        cm.github_stars - COALESCE(pdm.github_stars, 0) AS stars_change,
        cm.github_forks - COALESCE(pdm.github_forks, 0) AS forks_change,
        cm.pypi_downloads_daily - COALESCE(pdm.pypi_downloads_daily, 0) AS downloads_change,
        cm.open_issues - COALESCE(pdm.open_issues, 0) AS issues_change,
        CURRENT_TIMESTAMP() AS history_created_at
    FROM current_metrics AS cm
    LEFT JOIN previous_day_metrics AS pdm ON cm.technology_name = pdm.technology_name
)

SELECT * FROM final_model

{% else %}

-- First run, no history to compare
SELECT
    cm.technology_name,
    cm.github_repo,
    cm.pypi_package,
    cm.github_stars,
    cm.github_forks,
    cm.github_watchers,
    cm.pypi_downloads_daily,
    cm.pypi_downloads_weekly,
    cm.pypi_downloads_monthly,
    cm.open_issues,
    cm.contributors_count,
    cm.github_releases,
    cm.pypi_release_count,
    cm.fork_to_star_ratio,
    cm.stars_per_contributor,
    cm.popularity_tier,
    cm.usage_tier,
    cm.github_created_at,
    cm.github_updated_at,
    cm.latest_release_published_at,
    cm.latest_release_upload_time,
    cm.snapshot_date,
    cm.last_updated_at,
    0 AS stars_change,
    0 AS forks_change,
    0 AS downloads_change,
    0 AS issues_change,
    CURRENT_TIMESTAMP() AS history_created_at
FROM current_metrics AS cm

{% endif %}
