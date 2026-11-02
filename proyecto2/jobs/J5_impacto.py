"""J5 - Impacto en paraderos entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Para cada (service_id, stop_id): buses/dia = suma de n_salidas de los viajes
que pasan por el paradero en cada periodo. Luego full outer join A vs B ->
delta de buses/dia por paradero.

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J5_impacto.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import LongType

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
SILVER_DIR = Path(os.environ.get("SILVER_DIR", str(PROJECT_DIR / "silver" / "buses_rm")))
GOLD_DIR = Path(os.environ.get("GOLD_DIR", str(PROJECT_DIR / "gold" / "comparacion")))

PERIODO_A = "20260829"  # V167
PERIODO_B = "20261003"  # V168


@udf(returnType=LongType())
def to_seconds(t):
    """Convierte un tiempo GTFS 'H+:MM:SS' (puede superar las 24h) a segundos."""
    if t is None or str(t).strip() == "":
        return None
    parts = str(t).strip().split(":")
    h = int(float(parts[0]))
    m = int(float(parts[1])) if len(parts) > 1 else 0
    s = int(float(parts[2])) if len(parts) > 2 else 0
    return int(h * 3600 + m * 60 + s)


def trips_headway(spark, periodo):
    """(trip_id, service_id, route_id, direction_id, n_salidas) para un periodo."""
    df = (
        spark.read.parquet(str(SILVER_DIR / "frequencies"))
        .filter(col("periodo_feed") == periodo)
        .withColumn("ini_seg", udf("to_seconds")(col("start_time")))
        .withColumn("fin_seg", udf("to_seconds")(col("end_time")))
        .withColumn("headway_secs", col("headway_secs").cast("long"))
        .join(
            spark.read.parquet(str(SILVER_DIR / "trips")).filter(
                col("periodo_feed") == periodo
            ),
            ["periodo_feed", "trip_id"],
        )
        .select(
            "periodo_feed",
            "trip_id",
            "service_id",
            "route_id",
            "direction_id",
            "headway_secs",
            "ini_seg",
            "fin_seg",
        )
    )
    df = df.filter(
        col("fin_seg").isNotNull()
        & (col("fin_seg") > col("ini_seg"))
        & (col("headway_secs") > lit(0))
    )
    df = df.withColumn(
        "n_salidas",
        ((col("fin_seg") - col("ini_seg") - lit(1)) / col("headway_secs"))
        .cast("int")
        .cast("long")
        + lit(1),
    )
    df = df.filter(col("n_salidas") > lit(0))
    return df.select(
        "periodo_feed",
        "trip_id",
        "service_id",
        "route_id",
        "direction_id",
        "headway_secs",
        "n_salidas",
    )


def main():
    spark = (
        SparkSession.builder.appName("J5-impacto-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    def buses_dia(periodo):
        n = trips_headway(spark, periodo).select("trip_id", "service_id", "n_salidas")
        stop_times = (
            spark.read.parquet(str(SILVER_DIR / "stop_times"))
            .filter(col("periodo_feed") == periodo)
            .select("trip_id", "stop_id")
        )
        return (
            stop_times.join(n, "trip_id")
            .groupBy("service_id", "stop_id")
            .agg(col("n_salidas").sum().alias("buses_dia"))
        )

    bd_a = buses_dia(PERIODO_A)
    bd_b = buses_dia(PERIODO_B)
    print(f"[J5] buses/dia A ({PERIODO_A}): {bd_a.count()}")
    print(f"[J5] buses/dia B ({PERIODO_B}): {bd_b.count()}")

    comparado = bd_a.alias("a").join(bd_b.alias("b"), ["service_id", "stop_id"], "full_outer")

    df_delta = comparado.select(
        col("service_id"),
        col("stop_id"),
        col("buses_dia_a").alias("buses_dia_a"),
        col("buses_dia_b").alias("buses_dia_b"),
        (col("buses_dia_b").coalesce(lit(0)) - col("buses_dia_a").coalesce(lit(0))).alias(
            "delta_buses_dia"
        ),
    )

    df_delta.write.mode("overwrite").parquet(str(GOLD_DIR / "g_delta_paraderos"))
    print(f"[J5] g_delta_paraderos: {df_delta.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
