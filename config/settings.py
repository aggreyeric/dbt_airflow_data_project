"""Application settings and configuration"""

import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings loaded from environment variables"""
    
    # Snowflake Configuration - from environment variables
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    snowflake_user: str = os.getenv("SNOWFLAKE_USER", "")
    snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE", "DATA_ENGINEERING_PROJECT")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    snowflake_schema_raw: str = "RAW_DATA"
    snowflake_schema_staging: str = "STAGING"
    snowflake_schema_marts: str = "MARTS"
    
    # API Configuration
    github_token: str = os.getenv("GITHUB_TOKEN", "")
    
    # Target Technologies - GitHub repositories
    target_technologies: List[str] = [
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
    
    # PyPI Package Names
    pypi_packages: List[str] = [
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


settings = Settings()
