{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH dim_tech AS (
    SELECT * FROM {{ ref('dim_technologies') }}
),

metrics AS (
    SELECT
        technology_name,
        github_repo,
        pypi_package,
        
        -- Popularity metrics
        stars AS github_stars,
        forks AS github_forks,
        watchers AS github_watchers,
        downloads_last_day AS pypi_downloads_daily,
        downloads_last_week AS pypi_downloads_weekly,
        downloads_last_month AS pypi_downloads_monthly,
        
        -- Activity metrics
        open_issues,
        contributors_count,
        releases_count AS github_releases,
        pypi_release_count,
        
        -- Calculated metrics
        CASE 
            WHEN downloads_last_day > 0 
            THEN downloads_last_week / downloads_last_day 
            ELSE 0 
        END AS weekly_to_daily_ratio,
        
        CASE 
            WHEN downloads_last_week > 0 
            THEN downloads_last_month / downloads_last_week 
            ELSE 0 
        END AS monthly_to_weekly_ratio,
        
        CASE 
            WHEN stars > 0 
            THEN forks::FLOAT / stars 
            ELSE 0 
        END AS fork_to_star_ratio,
        
        CASE 
            WHEN contributors_count > 0 
            THEN stars::FLOAT / contributors_count 
            ELSE 0 
        END AS stars_per_contributor,
        
        -- Categorizations
        CASE 
            WHEN stars >= 30000 THEN 'Very Popular'
            WHEN stars >= 15000 THEN 'Popular'
            WHEN stars >= 5000 THEN 'Moderate'
            ELSE 'Emerging'
        END AS popularity_tier,
        
        CASE 
            WHEN downloads_last_month >= 10000000 THEN 'High Usage'
            WHEN downloads_last_month >= 1000000 THEN 'Medium Usage'
            WHEN downloads_last_month >= 100000 THEN 'Low Usage'
            ELSE 'Minimal Usage'
        END AS usage_tier,
        
        -- Dates
        github_created_at,
        github_updated_at,
        latest_release_published_at,
        latest_release_upload_time,
        last_updated_at,
        
        -- Snapshot date for historical tracking
        DATE(last_updated_at) AS snapshot_date
        
    FROM dim_tech
    WHERE github_repo IS NOT NULL 
       OR pypi_package IS NOT NULL
)

SELECT * FROM metrics
