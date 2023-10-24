from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'receptions',
    default_args=default_args,
    description='Transfer data receptions',
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=10),
)

    
def mongo_to_bigquery():
    # Menghubungkan ke MongoDB
    client = MongoClient('medtrack-dev-shard-00-00.f6aki.mongodb.net:27017')
    db = tnt
    collection = client.tnt.receptions

    # Mengambil data dari MongoDB
    mongo_data = list(collection.find())

    # Menghubungkan ke BigQuery
    bq_client = bigquery.Client()
    dataset_id = 'bf-medtrack-sbu-staging.bf_medtrack_stg_bronze'
    table_id = 'receptions'

    # Memasukkan data ke BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job = bq_client.load_table_from_json(mongo_data, dataset_id + '.' + table_id, job_config=job_config)
    job.result()  # Menunggu hingga pekerjaan selesai

mongo_to_bigquery_task = PythonOperator(
    task_id='mongo_to_bigquery_task',
    python_callable=mongo_to_bigquery,
    dag=dag,
)

mongo_to_bigquery_task
