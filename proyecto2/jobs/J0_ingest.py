"""J0 - Ingesta del GTFS unificado de buses RM hacia la capa Silver (Proyecto 2).

Lee los CSV del dataset unificado (dos periodos: periodo_feed A y B),
aplica validaciones basicas, convierte los tiempos a segundos y escribe
las tablas a Parquet particionado por periodo_feed (capa Silver).

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J0_ingest.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import LongType

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
DATASET_DIR = os.environ.get("DATASET_DIR", str(PROJECT_DIR / "dataset_buses_rm 2" / "dataset_buses_rm"))
SILVER_DIR = os.environ.get("SILVER_DIR", str(PROJECT_DIR / "silver" / "buses_rm"))

# (tabla, archivo, tiene_periodo_feed, columnas de tiempo a convertir)
TABLES = [
    ("trips", "trips_buses_rm.csv", True, ["start_time", "end_time"]),
    ("stop_times", "stop_times_buses_rm.csv", True, ["arrival_time", "departure_time"]),
    ("frequencies", "frequencies_buses_rm.csv", True, ["start_time", "end_time"]),
    ("routes", "routes_buses_rm.csv", True, []),
    ("shapes", "shapes_buses_rm.csv", True, []),
    ("stops", "stops_buses_rm.csv", True, []),
    ("calendar", "calendar_buses_rm.csv", True, []),
    ("calendar_dates", "calendar_dates_buses_rm.csv", True, []),
]


def to_seconds(t):
    """Convierte un tiempo GTFS 'H+:MM:SS' (puede superar las 24h) a segundos."""
    if t is None or str(t).strip() == "":
        return None
    parts = str(t).strip().split(":")
    h = int(float(parts[0]))
    m = int(float(parts[1])) if len(parts) > 1 else 0
    s = int(float(parts[2])) if len(parts) > 2 else 0
    return int(h * 3600 + m * 60 + s)


to_seconds_udf = udf(to_seconds, LongType())


def main():
    spark = (
        SparkSession.builder.appName("J0-ingest-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    for tabla, archivo, tiene_periodo, cols_tiempo in TABLES:
        df = spark.read.option("header", True).option("inferSchema", True).csv(
            f"{DATASET_DIR}/{archivo}"
        )

        out_path = f"{SILVER_DIR}/{tabla}"
        if tiene_periodo:
            df = df.withColumn("periodo_feed", col("periodo_feed").cast("string"))
            periodos = (
                df.select("periodo_feed").distinct().orderBy("periodo_feed").collect()
            )
            periodos = [r[0] for r in periodos]
            print(f"[{tabla}] periodos: {periodos}")
            writer = df.write.mode("overwrite").partitionBy("periodo_feed")
        else:
            periodos = None
            writer = df.write.mode("overwrite")

        for col_time in ["start_time", "end_time", "arrival_time", "departure_time"]:
            if col_time in df.columns:
                df = df.withColumn(f"{col_time}_secs", to_seconds_udf(col(col_time)))

        writer.parquet(out_path)
        print(f"[{tabla}] filas: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
