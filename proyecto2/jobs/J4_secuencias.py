"""J4 - Comparacion de secuencias de paradas entre periodos (Proyecto 2)."""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
SILVER_DIR = os.environ.get("SILVER_DIR", str(PROJECT_DIR / "silver" / "buses_rm"))
GOLD_DIR = os.environ.get("GOLD_DIR", str(PROJECT_DIR / "gold" / "comparacion"))

PERIODO_A = "20260829"
PERIODO_B = "20261003"


def main():
    spark = (
        SparkSession.builder.appName("J4-secuencias-buses-rm")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    st_a = (
        spark.read.parquet(f"{SILVER_DIR}/stop_times")
        .filter(col("periodo_feed") == PERIODO_A)
        .select("trip_id", "stop_sequence", "stop_id")
    )
    st_b = (
        spark.read.parquet(f"{SILVER_DIR}/stop_times")
        .filter(col("periodo_feed") == PERIODO_B)
        .select("trip_id", "stop_sequence", "stop_id")
    )

    trips_a = st_a.select("trip_id").distinct().withColumn("src_a", lit("A"))
    trips_b = st_b.select("trip_id").distinct().withColumn("src_b", lit("B"))

    j = trips_a.alias("a").join(
        trips_b.alias("b"), col("a.trip_id") == col("b.trip_id"), "full_outer"
    ).select(
        coalesce(col("a.trip_id"), col("b.trip_id")).alias("trip_id"),
        col("src_a"),
        col("src_b"),
    )

    j = j.withColumn(
        "estado",
        when(col("src_a").isNull(), lit("NUEVO"))
        .when(col("src_b").isNull(), lit("ELIMINADO"))
        .otherwise(lit("MODIFICADO_CANDIDATO")),
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
        .filter(col("stop_id_a") != col("stop_id_b"))
    )
    conteo = df_diff.groupBy("trip_id").count().withColumnRenamed("count", "n_difs")

    j2 = j.join(conteo, "trip_id", "left")
    df_cambios = j2.withColumn(
        "estado_final",
        when(
            col("estado") == lit("MODIFICADO_CANDIDATO"),
            when(coalesce(col("n_difs"), lit(0)) > lit(0), lit("MODIFICADO")).otherwise(lit("SIN_CAMBIO")),
        ).otherwise(col("estado")),
    ).select(
        col("trip_id"),
        col("src_a"),
        col("src_b"),
        col("estado_final").alias("estado"),
        coalesce(col("n_difs"), lit(0)).alias("n_difs"),
    )

    df_cambios.write.mode("overwrite").parquet(f"{GOLD_DIR}/g_secuencias_cambios")
    print(f"[J4] g_secuencias_cambios: {df_cambios.count()}")

    spark.stop()


if __name__ == "__main__":
    main()