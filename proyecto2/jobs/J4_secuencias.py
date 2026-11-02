"""J4 - Comparacion de secuencias de paradas entre periodos (Proyecto 2).

A = periodo_feed 20260829 (V167) | B = periodo_feed 20261003 (V168)

Compara la secuencia de paradas (trip_id + stop_sequence + stop_id) entre periodos
mediante full outer join por (trip_id, stop_sequence).

Estados por trip:
- NUEVO: solo existe en B.
- ELIMINADO: solo existe en A.
- MODIFICADO: existe en ambos y cambió la secuencia (hay posiciones distintas).
- SIN_CAMBIO: existe en ambos y no hay cambios.

Uso:
    spark-submit --master <cluster> --py-files <deps> \
        proyecto2/jobs/J4_secuencias.py
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


def main():
    spark = (
        SparkSession.builder.appName("J4-secuencias-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    st_a = (
        spark.read.parquet(str(SILVER_DIR / "stop_times"))
        .filter(col("periodo_feed") == PERIODO_A)
        .select("trip_id", "stop_sequence", "stop_id")
    )
    st_b = (
        spark.read.parquet(str(SILVER_DIR / "stop_times"))
        .filter(col("periodo_feed") == PERIODO_B)
        .select("trip_id", "stop_sequence", "stop_id")
    )

    trips_a = st_a.select("trip_id").distinct().withColumn("src", lit("A"))
    trips_b = st_b.select("trip_id").distinct().withColumn("src", lit("B"))

    j = trips_a.alias("a").join(trips_b.alias("b"), col("trip_id") == col("trip_id"), "full_outer")
    j = j.withColumn(
        "estado",
        when(col("src_a").isNull(), lit("NUEVO")).when(col("src_b").isNull(), lit("ELIMINADO")),
    )

    st_join = st_a.alias("a").join(
        st_b.alias("b"),
        (col("a.trip_id") == col("b.trip_id")) & (col("a.stop_sequence") == col("b.stop_sequence")),
        "inner",
    )
    df_diff = (
        st_join.select(
            col("a.trip_id").alias("trip_id"),
            col("a.stop_id").alias("stop_id_a"),
            col("b.stop_id").alias("stop_id_b"),
        )
        .withColumn("es_diferente", col("stop_id_a") != col("stop_id_b"))
        .filter(col("es_diferente"))
    )
    conteo = df_diff.groupBy("trip_id").count().withColumnRenamed("count", "n_difs")

    j2 = j.join(conteo, col("trip_id") == col("trip_id"), "left")
    df_cambios = j2.withColumn(
        "estado_final",
        when(
            col("estado") == lit("MODIFICADO"),
            when(col("n_difs") > lit(0), lit("MODIFICADO")).otherwise(lit("SIN_CAMBIO")),
        ).otherwise(col("estado")),
    ).select(
        col("trip_id"),
        col("src_a").alias("src_a"),
        col("src_b").alias("src_b"),
        col("estado_final").alias("estado"),
        col("n_difs").alias("n_difs"),
    )

    df_cambios.write.mode("overwrite").parquet(str(GOLD_DIR / "g_secuencias_cambios"))
    print(f"[J4] g_secuencias_cambios: {df_cambios.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
