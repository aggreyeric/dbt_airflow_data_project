WITH raw_github AS (
    SELECT 
        extracted_at,
        repo_name,
        raw_data
    FROM {{ source('raw_data', 'github_repos') }}
),

parsed_data AS (
    SELECT 
        extracted_at,
        repo_name,
        raw_data:repo_name::STRING AS repo_name_parsed,
        raw_data:full_name::STRING AS full_name,
        raw_data:description::STRING AS description,
        raw_data:language::STRING AS language,
        raw_data:stars::INTEGER AS stars,
        raw_data:forks::INTEGER AS forks,
        raw_data:watchers::INTEGER AS watchers,
        raw_data:open_issues::INTEGER AS open_issues,
        raw_data:size::INTEGER AS size,
        raw_data:created_at::TIMESTAMP AS created_at,
        raw_data:updated_at::TIMESTAMP AS updated_at,
        raw_data:pushed_at::TIMESTAMP AS pushed_at,
        raw_data:default_branch::STRING AS default_branch,
        raw_data:contributors_count::INTEGER AS contributors_count,
        raw_data:releases_count::INTEGER AS releases_count,
        raw_data:latest_release:tag_name::STRING AS latest_release_tag,
        raw_data:latest_release:published_at::TIMESTAMP AS latest_release_published_at,
        raw_data:topics AS topics,
        raw_data:license::STRING AS license
    FROM raw_github
)

SELECT 
    extracted_at,
    COALESCE(repo_name_parsed, repo_name) AS repo_name,
    full_name,
    description,
    language,
    COALESCE(stars, 0) AS stars,
    COALESCE(forks, 0) AS forks,
    COALESCE(watchers, 0) AS watchers,
    COALESCE(open_issues, 0) AS open_issues,
    COALESCE(size, 0) AS size,
    created_at,
    updated_at,
    pushed_at,
    default_branch,
    COALESCE(contributors_count, 0) AS contributors_count,
    COALESCE(releases_count, 0) AS releases_count,
    latest_release_tag,
    latest_release_published_at,
    topics,
    license
FROM parsed_data
