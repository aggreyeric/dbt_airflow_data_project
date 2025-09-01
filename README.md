# Data Engineering Pipeline Project

A comprehensive data engineering pipeline that extracts, transforms, and analyzes data from GitHub and PyPI APIs for 10 key data engineering technologies. Built with modern tools including Snowflake, dbt, Airflow, and Python.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Extraction    │    │   Data Warehouse│
│                 │    │                 │    │                 │
│ • GitHub API    │───▶│ • Python        │───▶│ • Snowflake     │
│ • PyPI API      │    │   Extractors    │    │ • Raw Data      │
└─────────────────┘    │ • Two-step      │    │   Tables        │
                       │   VARIANT       │    └─────────────────┘
                       │   insertion     │              │
                       └─────────────────┘              │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Orchestration │    │  Transformation │    │   Data Models   │
│                 │    │                 │    │                 │
│ • Apache Airflow│◀───│ • dbt Core      │◀───│ • Staging       │
│ • Daily Schedule│    │ • SQL Models    │    │ • Dimension     │
│ • Docker        │    │ • Tests &       │    │ • Fact Tables   │
└─────────────────┘    │   Documentation │    │ • Historical    │
                       └─────────────────┘    └─────────────────┘
```

## 🎯 Project Objectives

- **Extract** data from GitHub and PyPI APIs for 10 data engineering technologies
- **Load** data into Snowflake using optimized VARIANT column approach
- **Transform** raw data into analytics-ready models using dbt
- **Track** historical changes and trends over time
- **Orchestrate** the entire pipeline with Apache Airflow
- **Enable** rich analytics on technology popularity and growth

## 📊 Technologies Tracked

### GitHub Repositories

- `apache/airflow` - Apache Airflow
- `dbt-labs/dbt-core` - dbt Core
- `apache/spark` - Apache Spark
- `pandas-dev/pandas` - Pandas
- `sqlalchemy/sqlalchemy` - SQLAlchemy
- `great-expectations/great_expectations` - Great Expectations
- `prefecthq/prefect` - Prefect
- `apache/kafka` - Apache Kafka
- `snowflakedb/snowflake-connector-python` - Snowflake Connector
- `duckdb/duckdb` - DuckDB

### PyPI Packages

- `apache-airflow`, `dbt-core`, `pyspark`, `pandas`, `sqlalchemy`
- `great-expectations`, `prefect`, `kafka-python`
- `snowflake-connector-python`, `duckdb`

## 🛠️ Tech Stack

- **Data Warehouse**: Snowflake
- **Transformation**: dbt Core
- **Orchestration**: Apache Airflow
- **Language**: Python 3.11
- **Package Manager**: UV
- **Containerization**: Docker & Docker Compose
- **APIs**: GitHub REST API, PyPI Stats API

## 📁 Project Structure

```
example_data_engineering_project/
├── src/
│   ├── extractors/                    # Python data extractors
│   │   ├── github_extractor.py       # GitHub API extractor
│   │   └── pypi_extractor.py         # PyPI API extractor
│   ├── extractors_airbyte/           # PyAirbyte-based extractors
│   ├── config/                       # Configuration files
│   │   └── settings.py              # Snowflake connection config
│   └── data_engineering_pipeline/    # dbt project
│       ├── models/
│       │   ├── staging/              # Data cleaning models
│       │   │   ├── stg_github_repos.sql
│       │   │   └── stg_pypi_packages.sql
│       │   └── marts/                # Analytics models
│       │       ├── dim_technologies.sql      # Technology dimension
│       │       ├── fct_technology_metrics.sql # Current metrics
│       │       ├── fct_technology_history.sql # Historical tracking
│       │       └── fct_technology_trends.sql  # Trend analysis
│       ├── dbt_project.yml
│       └── profiles.yml
├── dags/
│   └── data_engineering_pipeline_dag.py  # Airflow DAG
├── docker-compose.yml                # Airflow infrastructure
├── setup.sql                        # Snowflake setup script
├── grant_permissions.sql            # User permissions
├── .env                             # Environment variables
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Snowflake account with admin access
- GitHub Personal Access Token
- Python 3.11+ (for local development)

### 1. Environment Setup

Create a `.env` file in the project root:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=DATA_PIPELINE_USER
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DATA_ENGINEERING_PROJECT
SNOWFLAKE_SCHEMA=RAW_DATA
SNOWFLAKE_ROLE=DATA_ENGINEERING_PROJECT_ROLE

# API Keys
GITHUB_TOKEN=your_github_token
```

### 2. Snowflake Setup

Run the setup scripts in your Snowflake console:

```sql
-- Run setup.sql to create database, schemas, and tables
-- Run grant_permissions.sql to set up user permissions
```

### 3. dbt Configuration

Set up your dbt profile in `~/.dbt/profiles.yml`:

```yaml
data_engineering_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account.region
      user: DATA_PIPELINE_USER
      password: your_password
      role: DATA_ENGINEERING_PROJECT_ROLE
      database: DATA_ENGINEERING_PROJECT
      warehouse: COMPUTE_WH
      schema: STAGING
      threads: 4
```

### 4. Start Airflow

```bash
# Initialize Airflow (first time only)
docker-compose up airflow-init

#
load_examples = False


# Start all services
docker-compose up -d

# Access Airflow UI at http://localhost:8080
# Username: airflow, Password: airflow
```

### 5. Run the Pipeline

1. Navigate to Airflow UI (http://localhost:8080)
2. Enable the `data_engineering_pipeline` DAG
3. Trigger a manual run or wait for the daily schedule

## 📈 Data Models

### Staging Models

- **`stg_github_repos`**: Cleaned GitHub repository data
- **`stg_pypi_packages`**: Cleaned PyPI package data

### Mart Models

- **`dim_technologies`**: Technology dimension table combining GitHub and PyPI data
- **`fct_technology_metrics`**: Current snapshot with KPIs and categorizations
- **`fct_technology_history`**: Daily historical snapshots with change tracking
- **`fct_technology_trends`**: Trend analysis with growth rates and rankings

### Key Metrics Tracked

- **GitHub**: Stars, forks, watchers, issues, pull requests, contributors
- **PyPI**: Download counts, recent downloads, project metadata
- **Calculated**: Growth rates, popularity rankings, trend indicators

## 🔄 Pipeline Flow

1. **Extract**: Parallel extraction from GitHub and PyPI APIs
2. **Validate**: Data quality checks on raw data
3. **Transform**: dbt models process data through staging to marts
4. **Test**: Automated data quality tests
5. **Document**: Generate dbt documentation
6. **Schedule**: Daily execution at midnight

## 💾 Historical Data Strategy

- **Full History Retention**: All daily snapshots preserved
- **Incremental Processing**: Only new/changed data processed
- **Cost Optimization**: ~$40/year estimated storage cost
- **Trend Analysis**: Rich historical analytics capabilities

## 🧪 Testing

The pipeline includes comprehensive testing:

```bash
# Run dbt tests
cd src/data_engineering_pipeline
dbt test --profiles-dir ~/.dbt

# Test individual extractors
cd src
python -m extractors.github_extractor
python -m extractors.pypi_extractor
```

## 📊 Analytics Queries

Example queries for analyzing the data:

```sql
-- Top 5 fastest growing technologies (by GitHub stars)
SELECT
    technology_name,
    github_stars_current,
    stars_growth_rate_7d,
    popularity_rank
FROM DATA_ENGINEERING_PROJECT.MARTS.FCT_TECHNOLOGY_TRENDS
WHERE snapshot_date = CURRENT_DATE()
ORDER BY stars_growth_rate_7d DESC
LIMIT 5;

-- Technology adoption trends over time
SELECT
    technology_name,
    snapshot_date,
    github_stars_current,
    pypi_downloads_recent
FROM DATA_ENGINEERING_PROJECT.MARTS.FCT_TECHNOLOGY_HISTORY
WHERE technology_name IN ('pandas', 'apache-airflow', 'dbt-core')
ORDER BY technology_name, snapshot_date;
```

## 🔧 Development

### Local Development Setup

```bash
# Install UV package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
cd src
uv sync

# Activate virtual environment
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows
```

### Running Components Individually

```bash
# Test extractors
python src/extractors/github_extractor.py
python src/extractors/pypi_extractor.py

# Run dbt models
cd src/data_engineering_pipeline
dbt run --profiles-dir ~/.dbt
dbt test --profiles-dir ~/.dbt
```

## 🚨 Troubleshooting

### Common Issues

1. **Snowflake Connection**: Verify credentials in `.env` file
2. **GitHub Rate Limits**: Ensure valid GitHub token is set
3. **dbt Compilation**: Check model dependencies and syntax
4. **Airflow Tasks**: Review logs in Airflow UI for detailed errors

### Logs and Monitoring

- **Airflow Logs**: Available in Airflow UI under each task
- **dbt Logs**: Generated during model runs
- **Extractor Logs**: Console output with detailed extraction info

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test thoroughly
4. Submit a pull request with detailed description

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Acknowledgments

- Built for Scaletech Platforms Data Engineer position
- Uses modern data engineering best practices
- Implements scalable, maintainable architecture
- Provides rich analytics capabilities for technology trend analysis

---

**Project Completion Date**: January 2025  
**Author**: Data Engineering Team  
**Contact**: For questions about this pipeline, please refer to the documentation or create an issue.
