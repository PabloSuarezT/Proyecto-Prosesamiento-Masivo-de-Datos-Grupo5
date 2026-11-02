"""J3 - Comparacion de headways entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Genera las salidas por trip (frequencies + n_salidas) para cada periodo y
compara headways y cantidades de salidas mediante full outer join:

- NUEVO / ELIMINADO / MODIFICADO / SIN_CAMBIO por trip.
- Delta de salidas por recorrido (route x direction x service).

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J3_headways.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when
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
    """Una fila por trip: (periodo, trip_id, service_id, route_id, direction_id, headway_secs, n_salidas)."""
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
        SparkSession.builder.appName("J3-headway-compare-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    df_a = trips_headway(spark, PERIODO_A)
    df_b = trips_headway(spark, PERIODO_B)
    print(f"[J3] trips A ({PERIODO_A}): {df_a.count()}")
    print(f"[J3] trips B ({PERIODO_B}): {df_b.count()}")

    comparado = df_a.alias("a").join(df_b.alias("b"), df_a.trip_id == df_b.trip_id, "full_outer")

    df_cambios = (
        comparado.select(
            col("trip_id"),
            col("service_id_a").alias("service_id_a"),
            col("service_id_b").alias("service_id_b"),
            col("route_id_a").alias("route_id_a"),
            col("route_id_b").alias("route_id_b"),
            col("direction_id_a").alias("direction_id_a"),
            col("direction_id_b").alias("direction_id_b"),
            col("headway_secs_a").alias("headway_a"),
            col("headway_secs_b").alias("headway_b"),
            col("n_salidas_a").alias("n_salidas_a"),
            col("n_salidas_b").alias("n_salidas_b"),
        )
        .withColumn(
            "estado",
            (
                when(col("service_id_a").isNull(), "NUEVO")
                .when(col("service_id_b").isNull(), "ELIMINADO")
                .when(
                    (
                        col("headway_a").isNotNull()
                        & col("headway_b").isNotNull()
                        & ((col("headway_a") - col("headway_b")) == lit(0))
                        & ((col("n_salidas_a") - col("n_salidas_b")) == lit(0))
                    ),
                    "SIN_CAMBIO",
                )
                .otherwise("MODIFICADO")
            ),
        )
    )

    df_cambios.write.mode("overwrite").parquet(str(GOLD_DIR / "g_headway_cambios"))
    print(f"[J3] g_headway_cambios: {df_cambios.count()}")

    resumen = (
        df_a.groupBy("route_id", "direction_id", "service_id")
        .agg(col("n_salidas").sum().alias("salidas_a"))
        .join(
            df_b.groupBy("route_id", "direction_id", "service_id")
            .agg(col("n_salidas").sum().alias("salidas_b")),
            ["route_id", "direction_id", "service_id"],
            "full_outer",
        )
    )
    resumen = resumen.select(
        col("route_id"),
        col("direction_id"),
        col("service_id"),
        col("salidas_a").alias("salidas_a"),
        col("salidas_b").alias("salidas_b"),
        (col("salidas_b").coalesce(lit(0)) - col("salidas_a").coalesce(lit(0))).alias(
            "delta_salidas"
        ),
    )
    resumen.write.mode("overwrite").parquet(str(GOLD_DIR / "g_recorrido_delta"))
    print(f"[J3] g_recorrido_delta: {resumen.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
