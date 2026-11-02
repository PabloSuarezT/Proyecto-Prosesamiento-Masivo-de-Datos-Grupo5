"""J7 - Changelog unificado de cambios entre periodos (Proyecto 2).

Consolida los cambios detectados por J1 (routes), J2 (stops) y J3 (headways)
en una sola tabla Gold: g_changelog.

Schema unificado:
    entidad, entidad_id, estado, campo, valor_antes, valor_despues, detalle

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J7_changelog.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, unionAll
from pyspark.sql.types import LongType

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
GOLD_DIR = Path(os.environ.get("GOLD_DIR", str(PROJECT_DIR / "gold" / "comparacion")))

PERIODO_A = "20260829"  # V167
PERIODO_B = "20261003"  # V168


def main():
    spark = (
        SparkSession.builder.appName("J7-changelog-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    # --- Routes (J1) ---
    routes = (
        spark.read.parquet(str(GOLD_DIR / "g_routes_cambios"))
        .filter(col("estado") != "SIN_CAMBIO")
        .select(
            lit("route").alias("entidad"),
            col("route_id").alias("entidad_id"),
            col("estado"),
            lit("route").alias("campo"),
            col("route_short_name_a").alias("valor_antes"),
            col("route_short_name_b").alias("valor_despues"),
            lit("n/a").alias("detalle"),
        )
    )

    # --- Stops (J2) ---
    stops = (
        spark.read.parquet(str(GOLD_DIR / "g_stops_cambios"))
        .filter(col("estado") != "SIN_CAMBIO")
        .select(
            lit("stop").alias("entidad"),
            col("stop_id").alias("entidad_id"),
            col("estado"),
            lit("stop").alias("campo"),
            col("stop_name_a").alias("valor_antes"),
            col("stop_name_b").alias("valor_despues"),
            col("movido").alias("detalle"),
        )
    )

    # --- Headways (J3) ---
    headways = (
        spark.read.parquet(str(GOLD_DIR / "g_headway_cambios"))
        .filter(col("estado") != "SIN_CAMBIO")
        .select(
            lit("trip_headway").alias("entidad"),
            col("trip_id").alias("entidad_id"),
            col("estado"),
            lit("headway_secs").alias("campo"),
            col("headway_a").alias("valor_antes"),
            col("headway_b").alias("valor_despues"),
            lit(None).alias("detalle"),
        )
        .unionAll(
            spark.read.parquet(str(GOLD_DIR / "g_headway_cambios"))
            .filter(
                (col("estado") == "MODIFICADO")
                & (col("n_salidas_a") != col("n_salidas_b"))
            )
            .select(
                lit("trip_headway").alias("entidad"),
                col("trip_id").alias("entidad_id"),
                lit("MODIFICADO").alias("estado"),
                lit("n_salidas").alias("campo"),
                col("n_salidas_a").alias("valor_antes"),
                col("n_salidas_b").alias("valor_despues"),
                lit(None).alias("detalle"),
            )
        )
    )

    changelog = routes.unionAll(stops).unionAll(headways)

    changelog.write.mode("overwrite").parquet(str(GOLD_DIR / "g_changelog"))
    print(f"[J7] g_changelog: {changelog.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
