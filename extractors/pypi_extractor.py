"""PyPI API extractor for data engineering package statistics"""

import json
import time
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import requests
import snowflake.connector
from loguru import logger

# Use environment variables directly instead of config module
from dotenv import load_dotenv
load_dotenv()


class PyPIExtractor:
    """Extract PyPI package statistics for data engineering technologies"""
    
    def __init__(self):
        self.base_url = "https://pypi.org/pypi"
        self.stats_url = "https://pypistats.org/api"
        self.packages = [
            "apache-airflow",
            "dbt-core",
            "pyspark", 
            "pandas",
            "sqlalchemy",
            "great-expectations",
            "prefect",
            "kafka-python",
            "snowflake-connector-python",
            "duckdb"
        ]
        
    def get_package_info(self, package_name: str) -> Dict[str, Any]:
        """Fetch package information from PyPI API"""
        try:
            # Get package metadata
            package_url = f"{self.base_url}/{package_name}/json"
            response = requests.get(package_url)
            response.raise_for_status()
            package_data = response.json()
            
            # Get download statistics (last 30 days)
            stats_url = f"{self.stats_url}/packages/{package_name}/recent"
            stats_response = requests.get(stats_url)
            download_stats = {}
            
            if stats_response.status_code == 200:
                stats_data = stats_response.json()
                download_stats = {
                    "downloads_last_day": stats_data.get("data", {}).get("last_day", 0),
                    "downloads_last_week": stats_data.get("data", {}).get("last_week", 0),
                    "downloads_last_month": stats_data.get("data", {}).get("last_month", 0)
                }
            
            # Extract comprehensive package information
            info = package_data.get("info", {})
            releases = package_data.get("releases", {})
            latest_version = info.get("version")
            
            # Get latest release info
            latest_release_info = {}
            if latest_version and latest_version in releases:
                latest_files = releases[latest_version]
                if latest_files:
                    latest_release_info = {
                        "upload_time": latest_files[0].get("upload_time"),
                        "python_version": latest_files[0].get("python_version"),
                        "size": latest_files[0].get("size"),
                        "filename": latest_files[0].get("filename")
                    }
            
            extracted_data = {
                "package_name": package_name,
                "version": latest_version,
                "summary": info.get("summary"),
                "description_content_type": info.get("description_content_type"),
                "home_page": info.get("home_page"),
                "author": info.get("author"),
                "author_email": info.get("author_email"),
                "maintainer": info.get("maintainer"),
                "license": info.get("license"),
                "keywords": info.get("keywords"),
                "classifiers": info.get("classifiers", []),
                "requires_dist": info.get("requires_dist", []),
                "requires_python": info.get("requires_python"),
                "project_urls": info.get("project_urls", {}),
                "release_count": len(releases),
                "latest_release_info": latest_release_info,
                **download_stats,
                "extracted_at": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"Successfully extracted data for {package_name}")
            return extracted_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {package_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {package_name}: {e}")
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
                CREATE TEMPORARY TABLE PYPI_PACKAGES_TEMP (
                    extracted_at TIMESTAMP,
                    package_name STRING,
                    raw_data STRING
                )
            """
            cursor.execute(temp_table_query)

            # Prepare data for batch insert into temp table
            insert_data = []
            for package_data in data:
                if package_data:
                    try:
                        json_str = json.dumps(package_data)
                        # Validate JSON is properly formed
                        json.loads(json_str)
                        insert_data.append((
                            datetime.now(timezone.utc),
                            package_data.get('package_name'),
                            json_str
                        ))
                    except (TypeError, ValueError) as e:
                        logger.error(f"JSON serialization/validation failed for {package_data.get('package_name')}: {e}")
                        continue

            if not insert_data:
                logger.warning("No valid data to insert after filtering.")
                return

            # Insert into temporary table with STRING column
            temp_insert_query = """
                INSERT INTO PYPI_PACKAGES_TEMP (extracted_at, package_name, raw_data)
                VALUES (%s, %s, %s)
            """
            cursor.executemany(temp_insert_query, insert_data)

            # Move data to final table with PARSE_JSON
            final_insert_query = """
                INSERT INTO DATA_ENGINEERING_PROJECT.RAW_DATA.PYPI_PACKAGES (extracted_at, package_name, raw_data)
                SELECT extracted_at, package_name, PARSE_JSON(raw_data)
                FROM PYPI_PACKAGES_TEMP
            """
            cursor.execute(final_insert_query)

            conn.commit()
            logger.info(f"Successfully saved {len(insert_data)} packages to Snowflake")

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
    
    def extract_all_packages(self) -> List[Dict[str, Any]]:
        """Extract data for all configured packages"""
        logger.info(f"Starting extraction for {len(self.packages)} packages")
        
        extracted_data = []
        for package in self.packages:
            logger.info(f"Extracting data for {package}")
            package_data = self.get_package_info(package)
            if package_data:
                extracted_data.append(package_data)
            
            # Rate limiting - be respectful to PyPI
            time.sleep(1)
        
        logger.info(f"Extraction completed. {len(extracted_data)} packages processed successfully")
        return extracted_data
    
    def run(self):
        """Main extraction process"""
        try:
            logger.info("Starting PyPI data extraction")
            data = self.extract_all_packages()
            
            if data:
                self.save_to_snowflake(data)
                logger.info("PyPI extraction completed successfully")
            else:
                logger.warning("No data extracted")
                
        except Exception as e:
            logger.error(f"PyPI extraction failed: {e}")
            raise


if __name__ == "__main__":
    extractor = PyPIExtractor()
    extractor.run()
