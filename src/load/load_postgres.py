import json
import os
import psycopg2
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "brasileirao-raw")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def read_from_minio(filename):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    response = client.get_object(MINIO_BUCKET, filename)
    return json.loads(response.read().decode("utf-8"))

def load():
    print("Iniciando carga no PostgreSQL...")
    today = datetime.now().strftime("%Y-%m-%d")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS partidas (
            match_id INTEGER PRIMARY KEY,
            matchday INTEGER,
            date VARCHAR(20),
            home_team VARCHAR(100),
            away_team VARCHAR(100),
            home_goals INTEGER,
            away_goals INTEGER,
            winner VARCHAR(100),
            referee VARCHAR(100),
            status VARCHAR(20)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS arbitros (
            referee VARCHAR(100) PRIMARY KEY,
            total_jogos INTEGER,
            empates INTEGER,
            media_gols FLOAT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS times (
            time VARCHAR(100) PRIMARY KEY,
            jogos_casa INTEGER,
            gols_marcados_casa INTEGER,
            gols_sofridos_casa INTEGER,
            jogos_fora INTEGER,
            gols_marcados_fora INTEGER,
            gols_sofridos_fora INTEGER
        )
    """)
    conn.commit()

    partidas = read_from_minio(f"processed/partidas/{today}/partidas.json")["partidas"]
    for p in partidas:
        cursor.execute("""
            INSERT INTO partidas VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (match_id) DO UPDATE SET
                home_goals=EXCLUDED.home_goals, away_goals=EXCLUDED.away_goals,
                winner=EXCLUDED.winner, referee=EXCLUDED.referee
        """, (p["match_id"], p["matchday"], p["date"], p["home_team"], p["away_team"],
              p["home_goals"], p["away_goals"], p["winner"], p["referee"], p["status"]))

    arbitros = read_from_minio(f"processed/arbitros/{today}/arbitros.json")["arbitros"]
    for a in arbitros:
        cursor.execute("""
            INSERT INTO arbitros VALUES (%s,%s,%s,%s)
            ON CONFLICT (referee) DO UPDATE SET
                total_jogos=EXCLUDED.total_jogos, empates=EXCLUDED.empates, media_gols=EXCLUDED.media_gols
        """, (a["referee"], a["total_jogos"], a["empates"], a["media_gols"]))

    times = read_from_minio(f"processed/times/{today}/times.json")["times"]
    for t in times:
        cursor.execute("""
            INSERT INTO times VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (time) DO UPDATE SET
                jogos_casa=EXCLUDED.jogos_casa, gols_marcados_casa=EXCLUDED.gols_marcados_casa
        """, (t["time"], t.get("jogos_casa", 0), t.get("gols_marcados_casa", 0),
              t.get("gols_sofridos_casa", 0), t.get("jogos_fora", 0),
              t.get("gols_marcados_fora", 0), t.get("gols_sofridos_fora", 0)))

    conn.commit()
    conn.close()
    print(f"Carregado: {len(partidas)} partidas, {len(arbitros)} arbitros, {len(times)} times")

if __name__ == "__main__":
    load()
