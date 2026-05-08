import json
import os
from minio import Minio
from google.cloud import bigquery, storage
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "brasileirao-raw")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "brasileirao-arbitragem")
GCP_BUCKET = os.getenv("GCP_BUCKET", "brasileirao-arbitragem-data-lake")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials/credentials.json"

def read_from_minio(filename):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    response = client.get_object(MINIO_BUCKET, filename)
    return json.loads(response.read().decode("utf-8"))

def upload_to_gcs(data, filename):
    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)
    blob = bucket.blob(f"brasileirao/{filename}")
    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")
    print(f"GCS: brasileirao/{filename}")

def load_to_bigquery(table_id, rows, schema):
    client = bigquery.Client()
    full_table = f"{GCP_PROJECT_ID}.analytics.{table_id}"
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_json(rows, full_table, job_config=job_config)
    job.result()
    print(f"BigQuery: {full_table} — {len(rows)} registros")

def load():
    print("Iniciando carga no GCP...")
    today = datetime.now().strftime("%Y-%m-%d")

    partidas = read_from_minio(f"processed/partidas/{today}/partidas.json")["partidas"]
    upload_to_gcs({"partidas": partidas}, f"partidas/{today}/partidas.json")
    load_to_bigquery("brasileirao_partidas", partidas, [
        bigquery.SchemaField("match_id", "INTEGER"),
        bigquery.SchemaField("matchday", "INTEGER"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("home_team", "STRING"),
        bigquery.SchemaField("away_team", "STRING"),
        bigquery.SchemaField("home_goals", "INTEGER"),
        bigquery.SchemaField("away_goals", "INTEGER"),
        bigquery.SchemaField("winner", "STRING"),
        bigquery.SchemaField("referee", "STRING"),
        bigquery.SchemaField("status", "STRING"),
    ])

    arbitros = read_from_minio(f"processed/arbitros/{today}/arbitros.json")["arbitros"]
    upload_to_gcs({"arbitros": arbitros}, f"arbitros/{today}/arbitros.json")
    load_to_bigquery("brasileirao_arbitros", arbitros, [
        bigquery.SchemaField("referee", "STRING"),
        bigquery.SchemaField("total_jogos", "INTEGER"),
        bigquery.SchemaField("empates", "INTEGER"),
        bigquery.SchemaField("media_gols", "FLOAT"),
    ])

    times = read_from_minio(f"processed/times/{today}/times.json")["times"]
    upload_to_gcs({"times": times}, f"times/{today}/times.json")
    load_to_bigquery("brasileirao_times", times, [
        bigquery.SchemaField("time", "STRING"),
        bigquery.SchemaField("jogos_casa", "INTEGER"),
        bigquery.SchemaField("gols_marcados_casa", "INTEGER"),
        bigquery.SchemaField("gols_sofridos_casa", "INTEGER"),
        bigquery.SchemaField("jogos_fora", "INTEGER"),
        bigquery.SchemaField("gols_marcados_fora", "INTEGER"),
        bigquery.SchemaField("gols_sofridos_fora", "INTEGER"),
    ])

    print("Carga no GCP concluida!")

if __name__ == "__main__":
    load()
