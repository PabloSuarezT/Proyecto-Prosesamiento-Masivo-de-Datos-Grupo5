"""J2 - Comparacion de paraderos entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Full outer join por stop_id sobre la capa Silver -> estados NUEVO / ELIMINADO /
MODIFICADO / SIN_CAMBIO, con campos comparables (stop_name, stop_lat, stop_lon,
stop_code, location_type) y flag `movido` (coordenadas cambiaron respecto a la otra version).

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J2_stops.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import LongType

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
SILVER_DIR = Path(os.environ.get("SILVER_DIR", str(PROJECT_DIR / "silver" / "buses_rm")))
GOLD_DIR = Path(os.environ.get("GOLD_DIR", str(PROJECT_DIR / "gold" / "comparacion")))

PERIODO_A = "20260829"  # V167
PERIODO_B = "20261003"  # V168

CAMPOS = ["stop_name", "stop_lat", "stop_lon", "stop_code", "location_type"]


def stops_periodo(spark, periodo):
    return (
        spark.read.parquet(str(SILVER_DIR / "stops"))
        .filter(col("periodo_feed") == periodo)
        .select("stop_id", *[col(c) for c in CAMPOS])
    )


def main():
    spark = (
        SparkSession.builder.appName("J2-stops-compare-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    df_a = stops_periodo(spark, PERIODO_A)
    df_b = stops_periodo(spark, PERIODO_B)
    print(f"[J2] stops A ({PERIODO_A}): {df_a.count()}")
    print(f"[J2] stops B ({PERIODO_B}): {df_b.count()}")

    comparado = df_a.alias("a").join(df_b.alias("b"), df_a.stop_id == df_b.stop_id, "full_outer")

    movido = (
        when(
            col("stop_lat_a").isNotNull()
            & col("stop_lat_b").isNotNull()
            & ((col("stop_lat_a") - col("stop_lat_b")) == lit(0)),
            lit(False),
        ).otherwise(lit(True))
    )

    exprs = [col("stop_id")]
    for c in CAMPOS:
        exprs.append(col(f"{c}_a").alias(f"{c}_a"))
        exprs.append(col(f"{c}_b").alias(f"{c}_b"))

    df_cambios = (
        comparado.select(*exprs)
        .withColumn(
            "estado",
            (
                when(col("stop_name_a").isNull(), "NUEVO")
                .when(col("stop_name_b").isNull(), "ELIMINADO")
                .when(
                    (
                        (col("stop_name_a") == col("stop_name_b"))
                        & (col("stop_lat_a") == col("stop_lat_b"))
                        & (col("stop_lon_a") == col("stop_lon_b"))
                        & (col("stop_code_a") == col("stop_code_b"))
                        & (col("location_type_a") == col("location_type_b"))
                    ),
                    "SIN_CAMBIO",
                )
                .otherwise("MODIFICADO")
            ),
        )
        .withColumn("movido", movido)
    )

    df_cambios.write.mode("overwrite").parquet(str(GOLD_DIR / "g_stops_cambios"))
    print(f"[J2] g_stops_cambios: {df_cambios.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
