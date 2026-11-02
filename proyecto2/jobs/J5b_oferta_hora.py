"""J5b - Oferta real por paradero y hora entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Expande las salidas (frequencies + k) -> join stop_times -> pasadas absolutas
(pasada_seg = salida_seg + t_rel) -> agrupa por (service_id, stop_id, hora_reloj)
-> buses por hora. Luego full outer join A vs B -> delta.

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J5b_oferta_hora.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, explode, sequence
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


def salidas(spark, periodo):
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
    df = df.withColumn("k", explode(sequence(lit(0), col("n_salidas") - lit(1))))
    return df.withColumn("salida_seg", col("ini_seg") + col("k") * col("headway_secs"))


def oferta(periodo, spark, salidas_df):
    stop_times = (
        spark.read.parquet(str(SILVER_DIR / "stop_times"))
        .filter(col("periodo_feed") == periodo)
        .select("trip_id", "stop_id", "departure_secs")
    )
    return (
        salidas_df.join(stop_times, "trip_id")
        .withColumn("pasada_seg", col("salida_seg") + col("departure_secs"))
        .withColumn("hora_servicio", (col("pasada_seg") / lit(3600)).cast("int"))
        .withColumn("hora_reloj", col("hora_servicio") % 24)
        .groupBy("service_id", "stop_id", "hora_reloj")
        .count()
        .withColumnRenamed("count", "buses")
    )


def main():
    spark = (
        SparkSession.builder.appName("J5b-oferta-hora-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    oferta_a = oferta(PERIODO_A, spark, salidas(spark, PERIODO_A))
    oferta_b = oferta(PERIODO_B, spark, salidas(spark, PERIODO_B))
    print(f"[J5b] oferta A ({PERIODO_A}): {oferta_a.count()}")
    print(f"[J5b] oferta B ({PERIODO_B}): {oferta_b.count()}")

    comparado = oferta_a.alias("a").join(oferta_b.alias("b"), ["service_id", "stop_id", "hora_reloj"], "full_outer")

    df_delta = comparado.select(
        col("service_id"),
        col("stop_id"),
        col("hora_reloj"),
        col("buses_a").alias("buses_a"),
        col("buses_b").alias("buses_b"),
        (col("buses_b").coalesce(lit(0)) - col("buses_a").coalesce(lit(0))).alias("delta_buses"),
    )

    df_delta.write.mode("overwrite").parquet(str(GOLD_DIR / "g_delta_oferta_hora"))
    print(f"[J5b] g_delta_oferta_hora: {df_delta.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
