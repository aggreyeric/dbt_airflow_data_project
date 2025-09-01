{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH raw_pypi AS (
    SELECT 
        extracted_at,
        package_name,
        raw_data
    FROM {{ source('raw_data', 'pypi_packages') }}
),

parsed_data AS (
    SELECT 
        extracted_at,
        package_name,
        raw_data:package_name::STRING AS package_name_parsed,
        raw_data:version::STRING AS version,
        raw_data:summary::STRING AS summary,
        raw_data:description_content_type::STRING AS description_content_type,
        raw_data:home_page::STRING AS home_page,
        raw_data:author::STRING AS author,
        raw_data:author_email::STRING AS author_email,
        raw_data:maintainer::STRING AS maintainer,
        raw_data:license::STRING AS license,
        raw_data:keywords::STRING AS keywords,
        raw_data:classifiers AS classifiers,
        raw_data:requires_dist AS requires_dist,
        raw_data:requires_python::STRING AS requires_python,
        raw_data:project_urls AS project_urls,
        raw_data:release_count::INTEGER AS release_count,
        raw_data:latest_release_info:upload_time::TIMESTAMP AS latest_release_upload_time,
        raw_data:latest_release_info:python_version::STRING AS latest_python_version,
        raw_data:latest_release_info:size::INTEGER AS latest_release_size,
        raw_data:latest_release_info:filename::STRING AS latest_filename,
        raw_data:downloads_last_day::INTEGER AS downloads_last_day,
        raw_data:downloads_last_week::INTEGER AS downloads_last_week,
        raw_data:downloads_last_month::INTEGER AS downloads_last_month
    FROM raw_pypi
)

SELECT 
    extracted_at,
    COALESCE(package_name_parsed, package_name) AS package_name,
    version,
    summary,
    description_content_type,
    home_page,
    author,
    author_email,
    maintainer,
    license,
    keywords,
    classifiers,
    requires_dist,
    requires_python,
    project_urls,
    COALESCE(release_count, 0) AS release_count,
    latest_release_upload_time,
    latest_python_version,
    COALESCE(latest_release_size, 0) AS latest_release_size,
    latest_filename,
    COALESCE(downloads_last_day, 0) AS downloads_last_day,
    COALESCE(downloads_last_week, 0) AS downloads_last_week,
    COALESCE(downloads_last_month, 0) AS downloads_last_month
FROM parsed_data
