import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# -----------------------------------------------------------------------------
# Configuración de Rutas y Parámetros
# -----------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = os.environ.get("SILVER_DIR", f"file://{PROJECT_DIR / 'build' / 'silver'}")
GOLD_DIR = os.environ.get("GOLD_DIR", f"file://{PROJECT_DIR / 'build' / 'gold'}")

PERIODO_A = os.environ.get("PERIODO_A", "20260829")
PERIODO_B = os.environ.get("PERIODO_B", "20261003")

# -----------------------------------------------------------------------------
# Funciones Auxiliares
# -----------------------------------------------------------------------------
def time_to_secs(col_name):
    """
    Convierte 'HH:MM:SS' a segundos del día (soporta horas GTFS > 24).
    """
    parts = F.split(F.col(col_name), ":")
    return (
        parts.getItem(0).cast("int") * 3600 +
        parts.getItem(1).cast("int") * 60 +
        parts.getItem(2).cast("int")
    )

def oferta(periodo, spark):
    """
    Calcula las salidas/viajes por hora cruzando stop_times + trips (Silver) con headways (Gold).
    """
    stop_times_path = f"{SILVER_DIR}/stop_times"
    trips_path = f"{SILVER_DIR}/trips"
    headways_path = f"{GOLD_DIR}/headways"

    stop_times_df = spark.read.parquet(stop_times_path).filter(F.col("periodo_feed") == periodo)
    trips_df = spark.read.parquet(trips_path).filter(F.col("periodo_feed") == periodo)
    headways_df = spark.read.parquet(headways_path).filter(F.col("periodo_feed") == periodo)

    # 1. Obtener la hora del día a partir de departure_time en stop_times
    st_periodo = stop_times_df \
        .withColumn("departure_time_secs", time_to_secs("departure_time")) \
        .withColumn("hora", (F.floor(F.col("departure_time_secs") / 3600) % 24).cast("int")) \
        .select("trip_id", "stop_id", "hora")

    # 2. Relacionar trip_id con route_id y direction_id mediante trips
    st_trips = st_periodo.join(
        trips_df.select("trip_id", "route_id", "direction_id"),
        on="trip_id",
        how="inner"
    )

    # 3. Cruzar con headways para validar la oferta del servicio
    joined = st_trips.join(
        headways_df.select("route_id", "direction_id", "avg_headway_secs"),
        on=["route_id", "direction_id"],
        how="inner"
    )

    # 4. Agrupar por hora para obtener salidas distintas
    oferta_hora = joined.groupBy("hora") \
        .agg(F.countDistinct("trip_id").alias("salidas_hora")) \
        .withColumn("periodo_feed", F.lit(str(periodo)))

    return oferta_hora

# -----------------------------------------------------------------------------
# Función Principal
# -----------------------------------------------------------------------------
def main():
    spark = SparkSession.builder \
        .appName("J5b-oferta-hora-buses-rm") \
        .getOrCreate()

    print(f"[J5b] Calculando oferta por hora para {PERIODO_A} y {PERIODO_B}")

    oferta_a = oferta(PERIODO_A, spark)
    oferta_b = oferta(PERIODO_B, spark)

    oferta_total = oferta_a.unionByName(oferta_b)

    output_path = f"{GOLD_DIR}/g_oferta_hora"
    print(f"[J5b] Escribiendo oferta por hora en Capa Gold: {output_path}")

    oferta_total.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("[J5b] Finalizado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()