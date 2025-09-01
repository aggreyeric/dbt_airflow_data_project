{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH github_data AS (
    SELECT * FROM {{ ref('stg_github_repos') }}
),

pypi_data AS (
    SELECT * FROM {{ ref('stg_pypi_packages') }}
),

github_latest AS (
    SELECT *
    FROM (
        SELECT 
            gd.*,
            ROW_NUMBER() OVER (PARTITION BY gd.repo_name ORDER BY gd.extracted_at DESC) AS rn
        FROM github_data gd
    )
    WHERE rn = 1
),

pypi_latest AS (
    SELECT *
    FROM (
        SELECT 
            pd.*,
            ROW_NUMBER() OVER (PARTITION BY pd.package_name ORDER BY pd.extracted_at DESC) AS rn
        FROM pypi_data pd
    )
    WHERE rn = 1
),

-- Technology mapping based on repository and package names
technology_mapping AS (
    SELECT 
        'Apache Airflow' AS technology_name,
        'apache/airflow' AS github_repo,
        'apache-airflow' AS pypi_package
    UNION ALL
    SELECT 'dbt', 'dbt-labs/dbt-core', 'dbt-core'
    UNION ALL
    SELECT 'Apache Spark', 'apache/spark', 'pyspark'
    UNION ALL
    SELECT 'Pandas', 'pandas-dev/pandas', 'pandas'
    UNION ALL
    SELECT 'SQLAlchemy', 'sqlalchemy/sqlalchemy', 'sqlalchemy'
    UNION ALL
    SELECT 'Great Expectations', 'great-expectations/great_expectations', 'great-expectations'
    UNION ALL
    SELECT 'Prefect', 'prefecthq/prefect', 'prefect'
    UNION ALL
    SELECT 'Apache Kafka', 'apache/kafka', 'kafka-python'
    UNION ALL
    SELECT 'Snowflake Connector', 'snowflakedb/snowflake-connector-python', 'snowflake-connector-python'
    UNION ALL
    SELECT 'DuckDB', 'duckdb/duckdb', 'duckdb'
),

combined_data AS (
    SELECT
        tm.technology_name,
        tm.github_repo,
        tm.pypi_package,
        
        -- GitHub metrics
        gh.stars,
        gh.forks,
        gh.watchers,
        gh.open_issues,
        gh.contributors_count,
        gh.releases_count,
        gh.language,
        gh.license,
        gh.created_at AS github_created_at,
        gh.updated_at AS github_updated_at,
        gh.latest_release_tag,
        gh.latest_release_published_at,
        gh.topics,
        
        -- PyPI metrics
        py.version AS current_version,
        py.downloads_last_day,
        py.downloads_last_week,
        py.downloads_last_month,
        py.release_count AS pypi_release_count,
        py.requires_python,
        py.author,
        py.summary,
        py.home_page,
        py.latest_release_upload_time,
        
        -- Metadata
        GREATEST(gh.extracted_at, py.extracted_at) AS last_updated_at,
        CURRENT_TIMESTAMP() AS mart_created_at
        
    FROM technology_mapping tm
    LEFT JOIN github_latest gh ON tm.github_repo = gh.repo_name
    LEFT JOIN pypi_latest py ON tm.pypi_package = py.package_name
)

SELECT * FROM combined_data
