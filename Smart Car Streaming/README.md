# Smart Car Real-Time Streaming Data Pipeline with Apache Kafka, Zookeeper, Spark, S3, AWS Glue and Athena

This project provides a containerized real-time data streaming pipeline to track and monitor the status of a car traveling from Lagos to Abuja. This pipeline leverages distributed data processing and cloud technologies such as Apache Kafka, Zookeeper, Apache Spark, Docker, and AWS services (Athena and Glue).

## Prerequisites
- AWS Account with appropriate permissions for S3, Glue, Athena, and Redshift.
- Reddit API credentials.
- Docker Installation
- Python 3.9 or higher


## System Setup
1. Clone the Data Engineering repository and navigate to Smart Car Streaming/
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