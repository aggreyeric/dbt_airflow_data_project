FROM apache/airflow:3.0.0

# Switch to root to install packages
USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Ensure pip user bin is on PATH for the airflow user (dbt installs here)
ENV PATH=/home/airflow/.local/bin:$PATH

# Copy requirements and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt dependencies are installed via requirements.txt; avoid duplicate installs