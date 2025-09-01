# Data Engineering Pipeline Project

A comprehensive data engineering pipeline that extracts, transforms, and analyzes data from GitHub and PyPI APIs for 10 key data engineering technologies. Built with modern tools including Snowflake, dbt, Airflow, and Python.

## Overview

This repository contains a compact, production-style data pipeline that ingests GitHub and PyPI data for 10 core data engineering technologies, lands it in Snowflake, transforms it with dbt, and orchestrates everything with Airflow.

## What’s Included

- Snowflake data warehouse (raw and modeled layers)
- dbt project for transformations and tests
- Airflow DAG to orchestrate extraction and dbt runs
- Python extractors for GitHub and PyPI

## Data Flow (High level)

1. Extract metadata from GitHub and PyPI for selected technologies
2. Land raw payloads in Snowflake
3. Transform to clean staging models
4. Build analytics marts for dimensions, facts, and trends
5. Schedule and monitor runs via Airflow

## Models

- Staging: cleaned GitHub and PyPI datasets
- Marts: technology dimension, current metrics, daily history, and trend outputs
 
## Quick Guide

- Prerequisites
  - Docker Desktop or Podman installed
  - Snowflake account and credentials
  - GitHub personal access token

- Configure
  - Create an environment file in the project root with Snowflake and GitHub values
  - Ensure the dbt profile exists at `dbt_project/profiles.yml` and matches your Snowflake setup (account, user, password, role, database, warehouse, schema, threads)

## Configuration sample (no password)

- dbt profile file: `dbt_project/profiles.yml`
  - profile: dbt_project
  - target: dev
  - outputs.dev
    - type: snowflake
    - account: nyjxxep-ixb31032 (example)
    - user: DATA_PIPELINE_USER
    - role: DATA_ENGINEERING_PROJECT_ROLE
    - database: DATA_ENGINEERING_PROJECT
    - warehouse: COMPUTE_WH
    - schema: DEV
    - threads: 4
    - password: set via environment variable only (do not store in file)

- Environment (no secrets committed)
  - SNOWFLAKE_ACCOUNT=nyjxxep-ixb31032
  - SNOWFLAKE_USER=DATA_PIPELINE_USER
  - SNOWFLAKE_DATABASE=DATA_ENGINEERING_PROJECT
  - SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  - SNOWFLAKE_ROLE=DATA_ENGINEERING_PROJECT_ROLE
  - SNOWFLAKE_SCHEMA=DEV
  - SNOWFLAKE_PASSWORD: set locally (e.g., in `.env`), not committed
  - GITHUB_TOKEN: set locally, not committed

- Start the stack
  - Build and start services using your container engine (Docker or Podman)
  - Access Airflow UI at http://localhost:8080 (no password I disabled it)

- Run the pipeline
  - In Airflow, enable the DAG `data_engineering_pipeline` and trigger a run
  - The DAG executes extraction, validation, dbt compile, staging and marts runs, tests, and docs generation

- Verify results
  - Check Snowflake database `DATA_ENGINEERING_PROJECT` (default schema `DEV`) for marts tables
  - Review dbt docs artifacts and Airflow task logs if needed
