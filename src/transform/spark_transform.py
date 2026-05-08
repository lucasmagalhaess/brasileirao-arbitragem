import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, count, round, lit
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "brasileirao-raw")

def read_from_minio(filename):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    response = client.get_object(MINIO_BUCKET, filename)
    return json.loads(response.read().decode("utf-8"))

def save_to_minio(data, filename):
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    json_data = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(MINIO_BUCKET, filename, BytesIO(json_data), length=len(json_data), content_type="application/json")
    print(f"Salvo: {filename}")

def transform():
    print("Iniciando transformacao com Spark...")
    spark = SparkSession.builder.appName("brasileirao-arbitragem").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    today = datetime.now().strftime("%Y-%m-%d")
    raw_data = read_from_minio(f"raw/matches/{today}/matches.json")
    matches = raw_data["matches"]
    print(f"Total de partidas: {len(matches)}")

    rows = []
    for m in matches:
        if m["status"] != "FINISHED":
            continue
        referee = None
        if m.get("referees"):
            for r in m["referees"]:
                if r.get("type") == "REFEREE" or len(m["referees"]) == 1:
                    referee = r.get("name")
                    break
        home = m["homeTeam"]["name"]
        away = m["awayTeam"]["name"]
        home_goals = m["score"]["fullTime"].get("home", 0) or 0
        away_goals = m["score"]["fullTime"].get("away", 0) or 0
        if home_goals > away_goals:
            winner = home
        elif away_goals > home_goals:
            winner = away
        else:
            winner = "Empate"
        rows.append({
            "match_id": m["id"],
            "matchday": m["matchday"],
            "date": m["utcDate"][:10],
            "home_team": home,
            "away_team": away,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "winner": winner,
            "referee": referee if referee else "Nao informado",
            "status": m["status"]
        })

    df = spark.createDataFrame(rows)
    print(f"Partidas finalizadas: {df.count()}")
    df.show(5, truncate=False)

    df_arbitros = df.filter(col("referee") != "Nao informado") \
        .groupBy("referee") \
        .agg(
            count("match_id").alias("total_jogos"),
            spark_sum(when(col("winner") == "Empate", 1).otherwise(0)).alias("empates"),
            round((spark_sum(col("home_goals")) + spark_sum(col("away_goals"))) / count("match_id"), 2).alias("media_gols")
        ).orderBy(col("total_jogos").desc())

    print("Top 10 arbitros por jogos apitados:")
    df_arbitros.show(10, truncate=False)

    df_home = df.groupBy("home_team").agg(
        count("match_id").alias("jogos_casa"),
        spark_sum(col("home_goals")).alias("gols_marcados_casa"),
        spark_sum(col("away_goals")).alias("gols_sofridos_casa")
    ).withColumnRenamed("home_team", "time")

    df_away = df.groupBy("away_team").agg(
        count("match_id").alias("jogos_fora"),
        spark_sum(col("away_goals")).alias("gols_marcados_fora"),
        spark_sum(col("home_goals")).alias("gols_sofridos_fora")
    ).withColumnRenamed("away_team", "time")

    df_times = df_home.join(df_away, "time", "outer").fillna(0)

    result_partidas = [row.asDict() for row in df.collect()]
    result_arbitros = [row.asDict() for row in df_arbitros.collect()]
    result_times = [row.asDict() for row in df_times.collect()]

    save_to_minio({"partidas": result_partidas}, f"processed/partidas/{today}/partidas.json")
    save_to_minio({"arbitros": result_arbitros}, f"processed/arbitros/{today}/arbitros.json")
    save_to_minio({"times": result_times}, f"processed/times/{today}/times.json")

    spark.stop()
    print("Transformacao concluida!")
    return result_arbitros

if __name__ == "__main__":
    transform()
