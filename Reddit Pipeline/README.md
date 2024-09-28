# Data Pipeline with Reddit using Airflow, Celery, Postgres, S3, AWS Glue, Athena, and Redshift

This project provides a containerized data pipeline solution to extract OnePiece subreddit posts using Reddit API, transform to valuable metrics, and load into a Redshift data warehouse. The pipeline leverages Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift. 

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)

## Overview

The pipeline is designed to:

1. Extract data from Reddit using its API.
2. Store the raw data in AWSS3 bucket from Airflow.
3. Transform the data using AWS Glue and Amazon Athena.
4. Load the transformed data into Amazon Redshift for analytics and querying.

## Prerequisites
- AWS Account with appropriate permissions for S3, Glue, Athena, and Redshift.
- Reddit API credentials.
- Docker Installation
- Python 3.9 or higher

## System Setup
1. Clone the Data Engineering repository and navigate to Reddit Pipeline/
   ```bash
    git clone https://github.com/yin-ye/data-engineering-portfolio.git
   ```
2. Create a virtual environment.
   ```bash
    python3 -m venv venv
   ```
3. Activate the virtual environment.
   ```bash
    source venv/bin/activate
   ```
4. Install the dependencies.
   ```bash
    pip install -r requirements.txt
   ``` 
5. Starting the containers
   ```bash
    docker-compose up -d
   ```
6. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```
