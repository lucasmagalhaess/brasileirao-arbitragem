import requests
import json
import os
from datetime import datetime
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FOOTBALL_API_KEY")
API_URL = os.getenv("FOOTBALL_API_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

HEADERS = {"X-Auth-Token": API_KEY}

def get_matches(season=2025):
    print(f"Buscando partidas do Brasileirao {season}...")
    url = f"{API_URL}/competitions/BSA/matches?season={season}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    print(f"Total de partidas: {data['resultSet']['count']}")
    return data

def get_standings(season=2025):
    print(f"Buscando classificacao {season}...")
    url = f"{API_URL}/competitions/BSA/standings?season={season}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def get_scorers(season=2025):
    print(f"Buscando artilheiros {season}...")
    url = f"{API_URL}/competitions/BSA/scorers?season={season}&limit=50"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def save_to_minio(data, filename):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    json_data = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        filename,
        BytesIO(json_data),
        length=len(json_data),
        content_type="application/json"
    )
    print(f"Salvo: {filename}")

def extract():
    today = datetime.now().strftime("%Y-%m-%d")
    print("Iniciando extracao do Brasileirao 2025...")

    matches = get_matches(2025)
    save_to_minio(matches, f"raw/matches/{today}/matches.json")

    standings = get_standings(2025)
    save_to_minio(standings, f"raw/standings/{today}/standings.json")

    scorers = get_scorers(2025)
    save_to_minio(scorers, f"raw/scorers/{today}/scorers.json")

    print("Extracao concluida!")
    return {
        "matches": len(matches["matches"]),
        "standings": "ok",
        "scorers": len(scorers.get("scorers", []))
    }

if __name__ == "__main__":
    result = extract()
    print(result)
