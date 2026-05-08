from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/src')

os.environ["FOOTBALL_API_KEY"] = "0a3cf8d26fec4c7786e5f974af4eb5c9"
os.environ["FOOTBALL_API_URL"] = "https://api.football-data.org/v4"
os.environ["MINIO_ENDPOINT"] = "minio:9000"
os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
os.environ["MINIO_SECRET_KEY"] = "minioadmin"
os.environ["MINIO_BUCKET"] = "brasileirao-raw"
os.environ["GCP_PROJECT_ID"] = "brasileirao-arbitragem"
os.environ["GCP_BUCKET"] = "brasileirao-arbitragem-data-lake"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials/credentials.json"

default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_brasileirao',
    default_args=default_args,
    description='Pipeline de dados de arbitragem do Brasileirao Serie A 2025',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['brasileirao', 'futebol', 'arbitragem', 'gcp'],
) as dag:

    def extract_task():
        from extract.football_api import extract
        extract()

    def transform_task():
        from transform.spark_transform import transform
        transform()

    def load_postgres_task():
        from load.load_postgres import load
        load()

    def load_gcp_task():
        from load.load_gcp import load
        load()

    t1 = PythonOperator(task_id='extrair_brasileirao', python_callable=extract_task)
    t2 = PythonOperator(task_id='transformar_com_spark', python_callable=transform_task)
    t3 = PythonOperator(task_id='carregar_postgresql', python_callable=load_postgres_task)
    t4 = PythonOperator(task_id='carregar_bigquery_gcs', python_callable=load_gcp_task)

    t1 >> t2 >> [t3, t4]
