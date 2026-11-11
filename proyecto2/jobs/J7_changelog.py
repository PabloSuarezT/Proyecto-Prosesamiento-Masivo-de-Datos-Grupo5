import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# -----------------------------------------------------------------------------
# Configuración de Rutas y Parámetros
# -----------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = os.environ.get("GOLD_DIR", f"file://{PROJECT_DIR / 'build' / 'gold'}")

PERIODO_A = os.environ.get("PERIODO_A", "20260829")
PERIODO_B = os.environ.get("PERIODO_B", "20261003")

def main():
    spark = SparkSession.builder \
        .appName("J7-changelog-buses-rm") \
        .getOrCreate()

    print(f"[J7] Generando Changelog consolidado entre {PERIODO_A} y {PERIODO_B}")

    # 1. Leer cambios de rutas
    routes_df = spark.read.parquet(f"{GOLD_DIR}/g_routes_cambios")

    # 2. Leer cambios de paradas
    stops_df = spark.read.parquet(f"{GOLD_DIR}/g_stops_cambios")

    # 3. Leer headways y calcular la diferencia/estado entre periodos
    headways_raw = spark.read.parquet(f"{GOLD_DIR}/headways")

    hw_a = headways_raw.filter(F.col("periodo_feed") == PERIODO_A) \
        .select(
            F.col("route_id"),
            F.col("direction_id"),
            F.col("avg_headway_secs").alias("hw_a"),
            F.col("total_trips").alias("trips_a")
        )

    hw_b = headways_raw.filter(F.col("periodo_feed") == PERIODO_B) \
        .select(
            F.col("route_id"),
            F.col("direction_id"),
            F.col("avg_headway_secs").alias("hw_b"),
            F.col("total_trips").alias("trips_b")
        )

    hw_joined = hw_a.join(hw_b, on=["route_id", "direction_id"], how="outer")

    hw_cambios = hw_joined.withColumn(
        "estado",
        F.when(F.col("hw_a").isNull(), F.lit("NUEVO"))
         .when(F.col("hw_b").isNull(), F.lit("ELIMINADO"))
         .when(F.abs(F.col("hw_b") - F.col("hw_a")) > 60, F.lit("MODIFICADO"))
         .otherwise(F.lit("SIN_CAMBIO"))
    ).filter(F.col("estado") != "SIN_CAMBIO")

    # 4. Normalizar columnas para realizar el Union Consolidado
    routes_changelog = routes_df.select(
        F.col("route_id").alias("entity_id"),
        F.col("estado"),
        F.lit("route").alias("tipo_cambio")
    )

    stops_changelog = stops_df.select(
        F.col("stop_id").alias("entity_id"),
        F.col("estado"),
        F.lit("stop").alias("tipo_cambio")
    )

    hw_changelog = hw_cambios.select(
        F.col("route_id").alias("entity_id"),
        F.col("estado"),
        F.lit("headway").alias("tipo_cambio")
    )

    changelog = routes_changelog \
        .unionByName(stops_changelog) \
        .unionByName(hw_changelog)

    output_path = f"{GOLD_DIR}/g_changelog"
    print(f"[J7] Escribiendo changelog consolidado en Capa Gold: {output_path}")

    changelog.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("[J7] Finalizado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()