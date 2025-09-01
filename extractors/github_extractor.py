"""GitHub API extractor for data engineering technologies"""

import os
import json
import time
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any
import requests
import snowflake.connector
from loguru import logger

# Use environment variables directly instead of config module
from dotenv import load_dotenv
load_dotenv()


class GitHubExtractor:
    """Extract GitHub repository metrics for data engineering technologies"""
    
    def __init__(self):
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"Bearer {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.repositories = [
            "apache/airflow",
            "dbt-labs/dbt-core", 
            "apache/spark",
            "pandas-dev/pandas",
            "sqlalchemy/sqlalchemy",
            "great-expectations/great_expectations",
            "prefecthq/prefect",
            "apache/kafka",
            "snowflakedb/snowflake-connector-python",
            "duckdb/duckdb"
        ]
        
    def get_repo_data(self, repo_name: str) -> Dict[str, Any]:
        """Fetch repository data from GitHub API"""
        try:
            # Get basic repository information
            repo_url = f"{self.base_url}/repos/{repo_name}"
            response = requests.get(repo_url, headers=self.headers)
            
            if response.status_code == 403:
                logger.warning(f"Rate limit hit for {repo_name}, waiting...")
                time.sleep(60)
                response = requests.get(repo_url, headers=self.headers)
            
            response.raise_for_status()
            repo_data = response.json()
            
            # Get additional metrics
            contributors_url = f"{self.base_url}/repos/{repo_name}/contributors"
            contributors_response = requests.get(contributors_url, headers=self.headers)
            contributors_count = len(contributors_response.json()) if contributors_response.status_code == 200 else 0
            
            # Get recent releases
            releases_url = f"{self.base_url}/repos/{repo_name}/releases"
            releases_response = requests.get(releases_url, headers=self.headers)
            releases = releases_response.json() if releases_response.status_code == 200 else []
            
            # Compile comprehensive data
            extracted_data = {
                "repo_name": repo_name,
                "full_name": repo_data.get("full_name"),
                "description": repo_data.get("description"),
                "language": repo_data.get("language"),
                "stars": repo_data.get("stargazers_count", 0),
                "forks": repo_data.get("forks_count", 0),
                "watchers": repo_data.get("watchers_count", 0),
                "open_issues": repo_data.get("open_issues_count", 0),
                "size": repo_data.get("size", 0),
                "created_at": repo_data.get("created_at"),
                "updated_at": repo_data.get("updated_at"),
                "pushed_at": repo_data.get("pushed_at"),
                "default_branch": repo_data.get("default_branch"),
                "contributors_count": contributors_count,
                "releases_count": len(releases),
                "latest_release": releases[0] if releases else None,
                "topics": repo_data.get("topics", []),
                "license": repo_data.get("license", {}).get("name") if repo_data.get("license") else None,
                "extracted_at": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"Successfully extracted data for {repo_name}")
            return extracted_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {repo_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {repo_name}: {e}")
            return None
    
    def get_snowflake_connection(self):
        """Create Snowflake connection"""
        return snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=os.getenv("SNOWFLAKE_DATABASE", "DATA_ENGINEERING_PROJECT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role='DATA_ENGINEERING_PROJECT_ROLE'
        )
    
    def save_to_snowflake(self, data: List[Dict[str, Any]]):
        """Save extracted data to Snowflake raw table using two-step approach"""
        if not data:
            logger.warning("No data to save")
            return

        conn = None
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()

            # Set schema context for temporary table creation
            cursor.execute("USE SCHEMA DATA_ENGINEERING_PROJECT.RAW_DATA")

            # Create temporary table with STRING column
            temp_table_query = """
                CREATE TEMPORARY TABLE GITHUB_REPOS_TEMP (
                    extracted_at TIMESTAMP,
                    repo_name STRING,
                    raw_data STRING
                )
            """
            cursor.execute(temp_table_query)

            # Prepare data for batch insert into temp table
            insert_data = []
            for repo_data in data:
                if repo_data:
                    try:
                        json_str = json.dumps(repo_data)
                        # Validate JSON is properly formed
                        json.loads(json_str)
                        insert_data.append((
                            datetime.now(timezone.utc),
                            repo_data.get('repo_name'),
                            json_str
                        ))
                    except (TypeError, ValueError) as e:
                        logger.error(f"JSON serialization/validation failed for {repo_data.get('repo_name')}: {e}")
                        continue

            if not insert_data:
                logger.warning("No valid data to insert after filtering.")
                return

            # Insert into temporary table with STRING column
            temp_insert_query = """
                INSERT INTO GITHUB_REPOS_TEMP (extracted_at, repo_name, raw_data)
                VALUES (%s, %s, %s)
            """
            cursor.executemany(temp_insert_query, insert_data)

            # Move data to final table with PARSE_JSON
            final_insert_query = """
                INSERT INTO DATA_ENGINEERING_PROJECT.RAW_DATA.GITHUB_REPOS (extracted_at, repo_name, raw_data)
                SELECT extracted_at, repo_name, PARSE_JSON(raw_data)
                FROM GITHUB_REPOS_TEMP
            """
            cursor.execute(final_insert_query)

            conn.commit()
            logger.info(f"Successfully saved {len(insert_data)} repositories to Snowflake")

        except Exception as e:
            logger.error(f"Error saving to Snowflake: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def extract_all_repositories(self) -> List[Dict[str, Any]]:
        """Extract data for all configured repositories"""
        logger.info(f"Starting extraction for {len(self.repositories)} repositories")
        
        extracted_data = []
        for repo in self.repositories:
            logger.info(f"Extracting data for {repo}")
            repo_data = self.get_repo_data(repo)
            if repo_data:
                extracted_data.append(repo_data)
            
            # Rate limiting - GitHub allows 5000 requests per hour
            time.sleep(1)
        
        logger.info(f"Extraction completed. {len(extracted_data)} repositories processed successfully")
        return extracted_data
    
    def run(self):
        """Main extraction process"""
        try:
            if not self.github_token:
                raise ValueError("GitHub token not provided. Set GITHUB_TOKEN environment variable.")
            
            logger.info("Starting GitHub data extraction")
            data = self.extract_all_repositories()
            
            if data:
                self.save_to_snowflake(data)
                logger.info("GitHub extraction completed successfully")
            else:
                logger.warning("No data extracted")
                
        except Exception as e:
            logger.error(f"GitHub extraction failed: {e}")
            raise


if __name__ == "__main__":
    extractor = GitHubExtractor()
    extractor.run()
