import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# -----------------------------------------------------------------------------
# Configuración de Rutas y Parámetros
# -----------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = os.environ.get("SILVER_DIR", f"file://{PROJECT_DIR / 'build' / 'silver'}")
GOLD_DIR = os.environ.get("GOLD_DIR", f"file://{PROJECT_DIR / 'build' / 'gold'}")

PERIODO_A = os.environ.get("PERIODO_A", "20260829")
PERIODO_B = os.environ.get("PERIODO_B", "20261003")

# -----------------------------------------------------------------------------
# Funciones Auxiliares
# -----------------------------------------------------------------------------
def largo_shapes(spark, periodo):
    """
    Calcula la longitud total de cada shape_id acumulando shape_dist_traveled o 
    calculando la distancia máxima registrada por trazado.
    """
    shapes_path = f"{SILVER_DIR}/shapes"
    shapes_df = spark.read.parquet(shapes_path).filter(F.col("periodo_feed") == periodo)

    # Agrupar por shape_id y obtener la distancia máxima (longitud del trazo)
    # Si la tabla tiene shape_dist_traveled se usa max; de lo contrario se calcula sum
    if "shape_dist_traveled" in shapes_df.columns:
        df_largo = shapes_df.groupBy("shape_id") \
            .agg(F.max("shape_dist_traveled").alias("largo_m"))
    elif "dist" in shapes_df.columns:
        df_largo = shapes_df.groupBy("shape_id") \
            .agg(F.sum("dist").alias("largo_m"))
    else:
        # Fallback si solo existen coordenadas (lat/lon)
        df_largo = shapes_df.groupBy("shape_id") \
            .agg(F.count("shape_pt_sequence").alias("largo_m"))

    return df_largo.withColumn("periodo_feed", F.lit(str(periodo)))

# -----------------------------------------------------------------------------
# Función Principal
# -----------------------------------------------------------------------------
def main():
    spark = SparkSession.builder \
        .appName("J6-shapes-compare-buses-rm") \
        .getOrCreate()

    print(f"[J6] Calculando métricas de shapes para {PERIODO_A} y {PERIODO_B}")

    largo_a = largo_shapes(spark, PERIODO_A)
    largo_b = largo_shapes(spark, PERIODO_B)

    shapes_total = largo_a.unionByName(largo_b)

    output_path = f"{GOLD_DIR}/g_shapes_compare"
    print(f"[J6] Escribiendo comparación de shapes en Capa Gold: {output_path}")

    shapes_total.write \
        .mode("overwrite") \
        .parquet(output_path)

    print("[J6] Finalizado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()