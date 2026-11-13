"""J0 - Ingesta del GTFS unificado de buses RM hacia la capa Silver (Proyecto 2).

Lee los CSV del dataset unificado (dos periodos: periodo_feed A y B),
aplica validaciones básicas, convierte los tiempos a segundos y escribe
las tablas a Parquet particionado por periodo_feed (capa Silver).

Uso:
    spark-submit jobs/J0_ingest.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import LongType

PROJECT_DIR = Path(__file__).resolve().parent.parent

# Rutas configurables por variables de entorno (por defecto usa las del proyecto)
DATASET_DIR = os.environ.get(
    "DATASET_DIR", 
    str(PROJECT_DIR / "dataset_buses_rm" / "dataset_buses_rm")
)
SILVER_DIR = os.environ.get(
    "SILVER_DIR", 
    str(PROJECT_DIR / "build" / "silver" / "buses_rm")
)

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
    try:
        parts = str(t).strip().split(":")
        h = int(float(parts[0]))
        m = int(float(parts[1])) if len(parts) > 1 else 0
        s = int(float(parts[2])) if len(parts) > 2 else 0
        return int(h * 3600 + m * 60 + s)
    except (ValueError, IndexError):
        return None


to_seconds_udf = udf(to_seconds, LongType())


def main():
    spark = (
        SparkSession.builder.appName("J0-ingest-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    print(f"[J0] Procesando desde: {DATASET_DIR}")
    print(f"[J0] Guardando en: {SILVER_DIR}")

    for tabla, archivo, tiene_periodo, cols_tiempo in TABLES:
        # Limpiar el prefijo file:// para que os.path.exists funcione bien localmente
        clean_dataset_dir = DATASET_DIR.replace("file://", "") if DATASET_DIR.startswith("file://") else DATASET_DIR
        input_file_path = os.path.join(clean_dataset_dir, archivo)
        
        if not os.path.exists(input_file_path):
            print(f"[{tabla}] WARN: No existe el archivo {input_file_path}, omitiendo...")
            continue

        # Spark lee usando la ruta original (con o sin file://)
        input_file_spark = f"{DATASET_DIR}/{archivo}"
        df = spark.read.option("header", True).option("inferSchema", True).csv(input_file_spark)

        # 1. Transformación de columnas de tiempo a segundos
        for col_time in cols_tiempo:
            if col_time in df.columns:
                df = df.withColumn(f"{col_time}_secs", to_seconds_udf(col(col_time)))

        out_path = f"{SILVER_DIR}/{tabla}"

        # 2. Escritura particionada si corresponde
        if tiene_periodo and "periodo_feed" in df.columns:
            df = df.withColumn("periodo_feed", col("periodo_feed").cast("string"))
            periodos = [r[0] for r in df.select("periodo_feed").distinct().orderBy("periodo_feed").collect()]
            print(f"[{tabla}] periodos: {periodos}")
            
            df.write.mode("overwrite").partitionBy("periodo_feed").parquet(out_path)
        else:
            df.write.mode("overwrite").parquet(out_path)

        print(f"[{tabla}] procesadas filas: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()