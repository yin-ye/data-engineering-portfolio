import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta

from utils.constants import EXTRACT_LIMIT

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': 'Yin Chukwuma',
    'start_date': datetime(2024, 5, 17),
    'email': ['chimuanyachukwuma@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# extraction from reddit
extract_reddit_posts = PythonOperator(
    task_id='extract_reddit_posts',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{datetime.now().strftime("%Y%m%d")}',
        'subreddit': 'OnePiece',
        'time_filter': 'day',
        'limit': EXTRACT_LIMIT
    },
    dag=dag
)

# upload to AWS s3
upload_to_aws_s3 = PythonOperator(
    task_id='upload_to_aws',
    python_callable=upload_s3_pipeline,
    dag=dag
)

extract_reddit_posts >> upload_to_aws_s3
