"""J1 - Comparacion de rutas entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Full outer join por route_id sobre la capa Silver -> estados NUEVO /
ELIMINADO / MODIFICADO / SIN_CAMBIO, con campos comparables
(route_short_name, route_long_name, route_desc, route_type, route_color,
route_text_color, route_url).

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J1_routes.py
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

CAMPOS = [
    "route_short_name",
    "route_long_name",
    "route_desc",
    "route_type",
    "route_color",
    "route_text_color",
    "route_url",
]


def routes_periodo(spark, periodo):
    return (
        spark.read.parquet(str(SILVER_DIR / "routes"))
        .filter(col("periodo_feed") == periodo)
        .select("route_id", *[col(c) for c in CAMPOS])
    )


def main():
    spark = (
        SparkSession.builder.appName("J1-routes-compare-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    df_a = routes_periodo(spark, PERIODO_A)
    df_b = routes_periodo(spark, PERIODO_B)
    print(f"[J1] routes A ({PERIODO_A}): {df_a.count()}")
    print(f"[J1] routes B ({PERIODO_B}): {df_b.count()}")

    comparado = df_a.alias("a").join(df_b.alias("b"), df_a.route_id == df_b.route_id, "full_outer")

    sin_cambio = (
        (col("route_short_name_a") == col("route_short_name_b"))
        & (col("route_long_name_a") == col("route_long_name_b"))
        & (col("route_desc_a") == col("route_desc_b"))
        & (col("route_type_a") == col("route_type_b"))
        & (col("route_color_a") == col("route_color_b"))
        & (col("route_text_color_a") == col("route_text_color_b"))
        & (col("route_url_a") == col("route_url_b"))
    )

    exprs = [col("route_id")]
    for c in CAMPOS:
        exprs.append(col(f"{c}_a").alias(f"{c}_a"))
        exprs.append(col(f"{c}_b").alias(f"{c}_b"))

    df_cambios = (
        comparado.select(*exprs)
        .withColumn(
            "estado",
            (
                when(col("route_short_name_a").isNull(), "NUEVO")
                .when(col("route_short_name_b").isNull(), "ELIMINADO")
                .when(sin_cambio, "SIN_CAMBIO")
                .otherwise("MODIFICADO")
            ),
        )
    )

    df_cambios.write.mode("overwrite").parquet(str(GOLD_DIR / "g_routes_cambios"))
    print(f"[J1] g_routes_cambios: {df_cambios.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
