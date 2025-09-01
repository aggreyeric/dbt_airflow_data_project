from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
# Removed deprecated days_ago import
import sys
import os

# Add the dags directory to Python path for imports
sys.path.append('/opt/airflow/dags')
sys.path.append('/opt/airflow/dags/extractors')
sys.path.append('/opt/airflow/config')

# dbt configuration
DBT_DIR = "/opt/airflow/dags/dbt_project"
DBT_ENV = os.environ.copy()
DBT_ENV["DBT_PROFILES_DIR"] = DBT_DIR
# Ensure pip user bin is on PATH so `dbt` is found in non-login shells
DBT_ENV["PATH"] = f"/home/airflow/.local/bin:{DBT_ENV.get('PATH','')}"

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    'data_engineering_pipeline',
    default_args=default_args,
    description='Complete data engineering pipeline for technology metrics',
    schedule='@daily',  # Run daily at midnight
    max_active_runs=1,
    tags=['data-engineering', 'github', 'pypi', 'snowflake', 'dbt'],
)

def extract_github_data(**context):
    """Extract data from GitHub API for data engineering technologies"""
    from github_extractor import GitHubExtractor
    
    extractor = GitHubExtractor()
    
    print(f"Starting GitHub data extraction for {len(extractor.repositories)} technologies")
    extractor.run()  # This calls extract_all_repositories() and save_to_snowflake() internally
    print(f"Successfully completed GitHub data extraction")
    
    return len(extractor.repositories)

def extract_pypi_data(**context):
    """Extract data from PyPI API for data engineering packages"""
    from pypi_extractor import PyPIExtractor
    
    extractor = PyPIExtractor()
    
    print(f"Starting PyPI data extraction for {len(extractor.packages)} packages")
    extractor.run()  # This calls extract_all_packages() and save_to_snowflake() internally
    print(f"Successfully completed PyPI data extraction")
    
    return len(extractor.packages)

def validate_raw_data(**context):
    """Validate that raw data was successfully loaded"""
    import snowflake.connector
    import os
    
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "DATA_ENGINEERING_PROJECT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role='DATA_ENGINEERING_PROJECT_ROLE'
    )
    cursor = conn.cursor()
    
    try:
        # Check GitHub data
        cursor.execute("""
            SELECT COUNT(*) FROM DATA_ENGINEERING_PROJECT.RAW_DATA.GITHUB_REPOS 
            WHERE DATE(extracted_at) = CURRENT_DATE()
        """)
        github_count = cursor.fetchone()[0]
        
        # Check PyPI data  
        cursor.execute("""
            SELECT COUNT(*) FROM DATA_ENGINEERING_PROJECT.RAW_DATA.PYPI_PACKAGES
            WHERE DATE(extracted_at) = CURRENT_DATE()
        """)
        pypi_count = cursor.fetchone()[0]
        
        print(f"Data validation results:")
        print(f"- GitHub repos extracted today: {github_count}")
        print(f"- PyPI packages extracted today: {pypi_count}")
        
        if github_count == 0 or pypi_count == 0:
            raise ValueError("No data found for today's extraction")
            
        return {"github_count": github_count, "pypi_count": pypi_count}
        
    finally:
        cursor.close()
        conn.close()

def check_dbt_models(**context):
    """Check that dbt models compiled successfully"""
    import subprocess
    import os

    # Change to dbt project directory
    os.chdir(DBT_DIR)

    # Run a clean compile sequence to avoid partial-parse cache issues
    commands = [
        ["dbt", "clean"],
        ["dbt", "deps"],
        ["dbt", "compile", "--no-partial-parse"],
    ]

    for args in commands:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            env=DBT_ENV,
        )
        if result.returncode != 0:
            print(f"dbt command failed: {' '.join(args)}")
            print(f"stdout:\n{result.stdout}")
            print(f"stderr:\n{result.stderr}")
            raise RuntimeError("dbt command failed")

    print("All dbt models compiled successfully")
    return True

# Define tasks
start_task = EmptyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Data extraction tasks (can run in parallel)
extract_github_task = PythonOperator(
    task_id='extract_github_data',
    python_callable=extract_github_data,
    dag=dag,
)

extract_pypi_task = PythonOperator(
    task_id='extract_pypi_data', 
    python_callable=extract_pypi_data,
    dag=dag,
)

# Data validation task
validate_data_task = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    dag=dag,
)

# dbt model check
check_models_task = PythonOperator(
    task_id='check_dbt_models',
    python_callable=check_dbt_models,
    dag=dag,
)

# Ensure dbt packages are installed
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='export PATH="$HOME/.local/bin:$PATH" && cd /opt/airflow/dags/dbt_project && dbt deps',
    env=DBT_ENV,
    dag=dag,
)

# dbt transformations
run_staging_models = BashOperator(
    task_id='run_staging_models',
    bash_command='export PATH="$HOME/.local/bin:$PATH" && cd /opt/airflow/dags/dbt_project && dbt run --models staging',
    env=DBT_ENV,
    dag=dag,
)

run_mart_models = BashOperator(
    task_id='run_mart_models', 
    bash_command='export PATH="$HOME/.local/bin:$PATH" && cd /opt/airflow/dags/dbt_project && dbt run --models marts',
    env=DBT_ENV,
    dag=dag,
)

# dbt tests
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command='export PATH="$HOME/.local/bin:$PATH" && cd /opt/airflow/dags/dbt_project && dbt test',
    env=DBT_ENV,
    dag=dag,
)

# Generate dbt documentation
generate_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='export PATH="$HOME/.local/bin:$PATH" && cd /opt/airflow/dags/dbt_project && dbt docs generate',
    env=DBT_ENV,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies
start_task >> [extract_github_task, extract_pypi_task]
[extract_github_task, extract_pypi_task] >> validate_data_task
validate_data_task >> check_models_task
check_models_task >> dbt_deps
dbt_deps >> run_staging_models
run_staging_models >> run_mart_models
run_mart_models >> run_dbt_tests
run_dbt_tests >> generate_docs
generate_docs >> end_task
