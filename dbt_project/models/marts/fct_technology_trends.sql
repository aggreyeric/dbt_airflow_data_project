{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH historical_data AS (
    SELECT * FROM {{ ref('fct_technology_metrics') }}
),

-- Calculate 7-day and 30-day trends
trend_calculations AS (
    SELECT 
        technology_name,
        snapshot_date,
        github_stars,
        github_forks,
        pypi_downloads_daily,
        
        -- 7-day moving averages
        AVG(github_stars) OVER (
            PARTITION BY technology_name 
            ORDER BY snapshot_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS stars_7day_avg,
        
        AVG(pypi_downloads_daily) OVER (
            PARTITION BY technology_name 
            ORDER BY snapshot_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS downloads_7day_avg,
        
        -- 30-day moving averages
        AVG(github_stars) OVER (
            PARTITION BY technology_name 
            ORDER BY snapshot_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS stars_30day_avg,
        
        -- Growth rates (7-day and 30-day)
        LAG(github_stars, 7) OVER (
            PARTITION BY technology_name 
            ORDER BY snapshot_date
        ) AS stars_7days_ago,
        
        LAG(github_stars, 30) OVER (
            PARTITION BY technology_name 
            ORDER BY snapshot_date
        ) AS stars_30days_ago,
        
        LAG(pypi_downloads_daily, 7) OVER (
            PARTITION BY technology_name 
            ORDER BY snapshot_date
        ) AS downloads_7days_ago,
        
        -- Rank by popularity each day
        RANK() OVER (
            PARTITION BY snapshot_date 
            ORDER BY github_stars DESC
        ) AS daily_popularity_rank
        
    FROM historical_data
),

final_trends AS (
    SELECT 
        technology_name,
        snapshot_date,
        github_stars,
        github_forks,
        pypi_downloads_daily,
        stars_7day_avg,
        downloads_7day_avg,
        stars_30day_avg,
        daily_popularity_rank,
        
        -- Calculate growth rates
        CASE 
            WHEN stars_7days_ago > 0 
            THEN ((github_stars - stars_7days_ago)::FLOAT / stars_7days_ago) * 100 
            ELSE 0 
        END AS stars_7day_growth_pct,
        
        CASE 
            WHEN stars_30days_ago > 0 
            THEN ((github_stars - stars_30days_ago)::FLOAT / stars_30days_ago) * 100 
            ELSE 0 
        END AS stars_30day_growth_pct,
        
        CASE 
            WHEN downloads_7days_ago > 0 
            THEN ((pypi_downloads_daily - downloads_7days_ago)::FLOAT / downloads_7days_ago) * 100 
            ELSE 0 
        END AS downloads_7day_growth_pct,
        
        -- Trend indicators
        CASE 
            WHEN github_stars > stars_7day_avg THEN 'Above Average'
            WHEN github_stars < stars_7day_avg THEN 'Below Average'
            ELSE 'Average'
        END AS stars_trend_indicator,
        
        CASE 
            WHEN pypi_downloads_daily > downloads_7day_avg THEN 'Above Average'
            WHEN pypi_downloads_daily < downloads_7day_avg THEN 'Below Average'
            ELSE 'Average'
        END AS downloads_trend_indicator
        
    FROM trend_calculations
)

SELECT * FROM final_trends
WHERE snapshot_date >= CURRENT_DATE - 90  -- Keep 90 days of trend data
