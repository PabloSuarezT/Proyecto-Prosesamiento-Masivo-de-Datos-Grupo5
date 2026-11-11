import os
import sys
from pathlib import Path
from datetime import datetime, time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# -----------------------------------------------------------------------------
# Configuración de Rutas y Parámetros
# -----------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = os.environ.get("SILVER_DIR", f"file://{PROJECT_DIR / 'build' / 'silver'}")
GOLD_DIR = os.environ.get("GOLD_DIR", f"file://{PROJECT_DIR / 'build' / 'gold'}")

PERIODO_A = os.environ.get("PERIODO_A", "20260829")
PERIODO_B = os.environ.get("PERIODO_B", "20261003")

# -----------------------------------------------------------------------------
# UDF: Conversión Universal de Tiempo a Segundos
# -----------------------------------------------------------------------------
def to_seconds(val):
    """
    Convierte tiempos a segundos desde el inicio del día.
    Acepta: strings ("08:30:00", "2026-11-11 08:30:00"), objetos datetime o time.
    """
    if val is None:
        return None

    # Si PySpark pasa un objeto datetime/time directamente
    if isinstance(val, (datetime, time)):
        return val.hour * 3600 + val.minute * 60 + val.second

    # Si pasa un string
    time_str = str(val).strip()
    if not time_str:
        return None

    # Aislar la hora si viene en formato Datetime ("2026-11-11 08:30:00" -> "08:30:00")
    if " " in time_str:
        time_str = time_str.split(" ")[-1]
    elif "T" in time_str:
        time_str = time_str.split("T")[-1]

    parts = time_str.split(":")
    if len(parts) < 3:
        return None

    try:
        h = int(float(parts[0]))
        m = int(float(parts[1]))
        s = int(float(parts[2]))
        return h * 3600 + m * 60 + s
    except (ValueError, TypeError):
        return None

# -----------------------------------------------------------------------------
# Función Principal
# -----------------------------------------------------------------------------
def main():
    spark = SparkSession.builder \
        .appName("J3-headway-compare-buses-rm") \
        .getOrCreate()

    to_seconds_udf = F.udf(to_seconds, IntegerType())

    print(f"[J3] Procesando frecuencias para períodos {PERIODO_A} y {PERIODO_B}")
    print(f"[J3] Leyendo capa Silver desde: {SILVER_DIR}")

    frequencies_path = f"{SILVER_DIR}/frequencies"
    trips_path = f"{SILVER_DIR}/trips"

    freq_df = spark.read.parquet(frequencies_path)
    trips_df = spark.read.parquet(trips_path)

    # Filtrar frecuencias válidas
    freq_filtered = freq_df.filter(
        (F.col("periodo_feed").isin(PERIODO_A, PERIODO_B)) &
        F.col("headway_secs").isNotNull() &
        (F.col("headway_secs") > 0) &
        F.col("trip_id").isNotNull()
    )

    trips_filtered = trips_df.filter(
        F.col("periodo_feed").isin(PERIODO_A, PERIODO_B)
    ).select("periodo_feed", "trip_id", "route_id", "direction_id")

    # Asegurar formateo a string "HH:mm:ss" antes de pasar a la UDF
    freq_formatted = freq_filtered \
        .withColumn("start_str", F.date_format(F.col("start_time"), "HH:mm:ss")) \
        .withColumn("end_str", F.date_format(F.col("end_time"), "HH:mm:ss"))

    # Si date_format retorna nulo (ej. si la columna ya era string y no timestamp), usar valor original
    freq_formatted = freq_formatted \
        .withColumn("start_str", F.coalesce(F.col("start_str"), F.col("start_time").cast("string"))) \
        .withColumn("end_str", F.coalesce(F.col("end_str"), F.col("end_time").cast("string")))

    # Aplicar UDF de segundos
    freq_with_secs = freq_formatted \
        .withColumn("start_seconds", to_seconds_udf(F.col("start_str"))) \
        .withColumn("end_seconds", to_seconds_udf(F.col("end_str")))

    # Join con Trips
    joined_df = freq_with_secs.join(
        trips_filtered,
        on=["periodo_feed", "trip_id"],
        how="inner"
    )

    # Resumen agrupado
    headways_summary = joined_df.groupBy("periodo_feed", "route_id", "direction_id") \
        .agg(
            F.avg("headway_secs").alias("avg_headway_secs"),
            F.min("start_seconds").alias("earliest_start_secs"),
            F.max("end_seconds").alias("latest_end_secs"),
            F.count("trip_id").alias("total_trips")
        )

    df_a = headways_summary.filter(F.col("periodo_feed") == PERIODO_A)
    df_b = headways_summary.filter(F.col("periodo_feed") == PERIODO_B)

    print(f"[J3] Resumen Headways A ({PERIODO_A}): {df_a.count()} registros")
    print(f"[J3] Resumen Headways B ({PERIODO_B}): {df_b.count()} registros")

    output_path = f"{GOLD_DIR}/headways"
    print(f"[J3] Escribiendo resultados en Capa Gold: {output_path}")

    headways_summary.write \
        .mode("overwrite") \
        .partitionBy("periodo_feed") \
        .parquet(output_path)

    print("[J3] Finalizado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()