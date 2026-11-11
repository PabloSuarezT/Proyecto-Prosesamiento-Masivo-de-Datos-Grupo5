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

# -----------------------------------------------------------------------------
# Función Principal
# -----------------------------------------------------------------------------
def main():
    spark = SparkSession.builder \
        .appName("J5-impacto-buses-rm") \
        .getOrCreate()

    print(f"[J5] Calculando métricas de impacto entre {PERIODO_A} y {PERIODO_B}")
    print(f"[J5] Leyendo capa Gold desde: {GOLD_DIR}")

    headways_path = f"{GOLD_DIR}/headways"
    headways_df = spark.read.parquet(headways_path)

    # Filtrar por los dos períodos de interés
    df_ab = headways_df.filter(F.col("periodo_feed").isin(PERIODO_A, PERIODO_B))

    # Calcular salidas aproximadas por servicio/sentido
    # n_salidas = (latest_end_secs - earliest_start_secs) / avg_headway_secs
    df_salidas = df_ab.withColumn(
        "span_secs", 
        F.greatest(F.lit(0), F.col("latest_end_secs") - F.col("earliest_start_secs"))
    ).withColumn(
        "n_salidas",
        F.when(
            (F.col("avg_headway_secs").isNotNull()) & (F.col("avg_headway_secs") > 0),
            F.col("span_secs") / F.col("avg_headway_secs")
        ).otherwise(0.0)
    )

    # Calcular total de buses/día para un período dado
    def buses_dia(periodo):
        res = df_salidas.filter(F.col("periodo_feed") == periodo) \
            .agg(F.sum("n_salidas").alias("buses_dia")) \
            .collect()
        return res[0]["buses_dia"] if res and res[0]["buses_dia"] is not None else 0.0

    bd_a = buses_dia(PERIODO_A)
    bd_b = buses_dia(PERIODO_B)

    diff_buses = bd_b - bd_a
    pct_buses = (diff_buses / bd_a * 100.0) if bd_a > 0 else 0.0

    print(f"[J5] Buses/día {PERIODO_A}: {bd_a:.2f}")
    print(f"[J5] Buses/día {PERIODO_B}: {bd_b:.2f}")
    print(f"[J5] Diferencia: {diff_buses:+.2f} ({pct_buses:+.2f}%)")

    # Guardar métricas de resumen en Parquet
    impacto_summary = spark.createDataFrame([
        (PERIODO_A, bd_a),
        (PERIODO_B, bd_b)
    ], ["periodo_feed", "buses_dia_total"])

    output_path = f"{GOLD_DIR}/g_impacto_buses"
    print(f"[J5] Escribiendo resultados en Capa Gold: {output_path}")

    impacto_summary.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("[J5] Finalizado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()