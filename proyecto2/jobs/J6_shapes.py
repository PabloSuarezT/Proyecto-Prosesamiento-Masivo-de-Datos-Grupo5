"""J6 - Comparacion de shapes entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Para cada (periodo, shape_id): largo del shape = suma de distancias euclidianas
entre vertices consecutivos (Window lag sobre shape_pt_sequence).

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J6_shapes.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, sqrt, pow, lag
from pyspark.sql.types import LongType

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
SILVER_DIR = Path(os.environ.get("SILVER_DIR", str(PROJECT_DIR / "silver" / "buses_rm")))
GOLD_DIR = Path(os.environ.get("GOLD_DIR", str(PROJECT_DIR / "gold" / "comparacion")))

PERIODO_A = "20260829"  # V167
PERIODO_B = "20261003"  # V168


def largo_shapes(spark, periodo):
    df = (
        spark.read.parquet(str(SILVER_DIR / "shapes"))
        .filter(col("periodo_feed") == periodo)
        .select("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence")
    )
    w = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
    return (
        df.withColumn("prev_lat", lag("shape_pt_lat").over(w))
        .withColumn("prev_lon", lag("shape_pt_lon").over(w))
        .filter(col("prev_lat").isNotNull())
        .withColumn(
            "dist",
            sqrt(
                pow(col("shape_pt_lat") - col("prev_lat"), 2)
                + pow(col("shape_pt_lon") - col("prev_lon"), 2)
            ),
        )
        .groupBy("shape_id")
        .agg(col("dist").sum().alias("largo"))
    )


def main():
    spark = (
        SparkSession.builder.appName("J6-shapes-compare-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    largo_a = largo_shapes(spark, PERIODO_A)
    largo_b = largo_shapes(spark, PERIODO_B)
    print(f"[J6] shapes A ({PERIODO_A}): {largo_a.count()}")
    print(f"[J6] shapes B ({PERIODO_B}): {largo_b.count()}")

    comparado = largo_a.alias("a").join(largo_b.alias("b"), "shape_id", "full_outer")

    df_cambios = comparado.select(
        col("shape_id"),
        col("largo_a").alias("largo_a"),
        col("largo_b").alias("largo_b"),
        (col("largo_b").coalesce(lit(0)) - col("largo_a").coalesce(lit(0))).alias("delta_largo"),
    )

    df_cambios.write.mode("overwrite").parquet(str(GOLD_DIR / "g_shapes_cambios"))
    print(f"[J6] g_shapes_cambios: {df_cambios.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
