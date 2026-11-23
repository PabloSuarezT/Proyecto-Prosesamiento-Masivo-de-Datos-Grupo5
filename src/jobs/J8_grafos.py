"""J8 - Análisis topológico de grafos entre períodos con GraphFrames.

Construye el grafo dirigido de paraderos (stops -> stop_times + trips) para
PERIODO_A y PERIODO_B desde la capa Silver y guarda en Gold:
  - g_grafos_nodos_cambios   : grados, PageRank, componente y deltas por paradero
  - g_grafos_aristas_cambios : tramos (src -> dst) nuevos, eliminados o modificados
  - g_grafos_resumen         : métricas globales de la red por período

Uso local (test_data):
    spark-submit --master local[*] \
      --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
      src/jobs/J8_grafos.py
"""

import os
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = os.environ.get("SILVER_DIR", str(PROJECT_DIR / "build" / "silver" / "buses_rm"))
GOLD_DIR = os.environ.get("GOLD_DIR", str(PROJECT_DIR / "build" / "gold" / "comparacion"))
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", str(PROJECT_DIR / "build" / "checkpoints"))

PERIODO_A = os.environ.get("PERIODO_A", "20260829")
PERIODO_B = os.environ.get("PERIODO_B", "20261003")
PAGERANK_MAX_ITER = int(os.environ.get("PAGERANK_MAX_ITER", "10"))
PAGERANK_RESET_PROB = float(os.environ.get("PAGERANK_RESET_PROB", "0.15"))


def construir_y_analizar_grafo(spark, periodo):
    """Construye el GraphFrame de un período y calcula métricas de nodos, aristas y resumen."""
    stops_df = (
        spark.read.parquet(f"{SILVER_DIR}/stops")
        .filter(F.col("periodo_feed") == str(periodo))
        .select(
            F.col("stop_id").cast("string").alias("id"),
            "stop_name",
            "stop_lat",
            "stop_lon",
        )
        .dropDuplicates(["id"])
    )

    stop_times_df = (
        spark.read.parquet(f"{SILVER_DIR}/stop_times")
        .filter(F.col("periodo_feed") == str(periodo))
        .select(
            "trip_id",
            F.col("stop_sequence").cast("int").alias("stop_sequence"),
            F.col("stop_id").cast("string").alias("stop_id"),
        )
    )

    trips_df = (
        spark.read.parquet(f"{SILVER_DIR}/trips")
        .filter(F.col("periodo_feed") == str(periodo))
        .select("trip_id", "route_id")
        .dropDuplicates(["trip_id"])
    )

    # Enriquecer stop_times con route_id (left join para no perder tramos en test_data)
    st_enriched = stop_times_df.join(trips_df, on="trip_id", how="left")

    # Generar aristas dirigidas (paradero actual -> paradero siguiente en el mismo viaje)
    w = Window.partitionBy("trip_id").orderBy("stop_sequence")
    edges_df = (
        st_enriched.withColumn("dst", F.lead("stop_id").over(w))
        .filter(F.col("dst").isNotNull() & (F.col("stop_id") != F.col("dst")))
        .groupBy(F.col("stop_id").alias("src"), "dst")
        .agg(
            F.countDistinct("trip_id").alias("n_trips"),
            F.countDistinct("route_id").alias("n_routes"),
        )
    )

    # Si no hay datos para este período, avisar y retornar None
    if not stops_df.take(1) or not edges_df.take(1):
        print(f"[J8] WARN: Período {periodo} sin nodos o aristas suficientes.")
        return None, None, None

    g = GraphFrame(stops_df, edges_df)

    # 1. Grados (entrada, salida, total)
    in_deg = g.inDegrees.withColumnRenamed("inDegree", "in_degree")
    out_deg = g.outDegrees.withColumnRenamed("outDegree", "out_degree")
    deg = g.degrees.withColumnRenamed("degree", "degree")

    # 2. Centralidad (PageRank)
    pr = g.pageRank(
        resetProbability=PAGERANK_RESET_PROB,
        maxIter=PAGERANK_MAX_ITER,
    ).vertices.select("id", "pagerank")

    # 3. Conectividad global (Componentes conexas)
    cc = g.connectedComponents(algorithm="graphx").select("id", "component")

    nodos_metricas = (
        stops_df.join(in_deg, "id", "left")
        .join(out_deg, "id", "left")
        .join(deg, "id", "left")
        .join(pr, "id", "left")
        .join(cc, "id", "left")
        .fillna({"in_degree": 0, "out_degree": 0, "degree": 0, "pagerank": 0.0})
        .withColumn("periodo_feed", F.lit(str(periodo)))
        .localCheckpoint(eager=True)
    )

    edges_periodo = edges_df.withColumn(
        "periodo_feed", F.lit(str(periodo))
    ).localCheckpoint(eager=True)

    # Resumen global del período
    resumen_row = nodos_metricas.agg(
        F.count("id").alias("n_nodos"),
        F.avg("degree").alias("grado_promedio"),
        F.max("pagerank").alias("max_pagerank"),
        F.countDistinct("component").alias("n_componentes"),
    ).collect()[0]

    n_aristas = edges_periodo.count()
    resumen = (
        str(periodo),
        int(resumen_row["n_nodos"]),
        int(n_aristas),
        int(resumen_row["n_componentes"]),
        float(resumen_row["grado_promedio"] or 0.0),
        float(resumen_row["max_pagerank"] or 0.0),
    )

    return nodos_metricas, edges_periodo, resumen


def main():
    spark = (
        SparkSession.builder.appName("J8-grafos-buses-rm")
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

    print(f"[J8] Leyendo Silver desde: {SILVER_DIR}")
    print(f"[J8] Comparando períodos: A={PERIODO_A} vs B={PERIODO_B}")

    # Mostrar períodos disponibles en Silver para facilitar pruebas con test_data
    periodos_disponibles = [
        r[0]
        for r in spark.read.parquet(f"{SILVER_DIR}/stops")
        .select("periodo_feed")
        .distinct()
        .orderBy("periodo_feed")
        .collect()
    ]
    print(f"[J8] Períodos encontrados en Silver: {periodos_disponibles}")

    nodos_a, edges_a, res_a = construir_y_analizar_grafo(spark, PERIODO_A)
    nodos_b, edges_b, res_b = construir_y_analizar_grafo(spark, PERIODO_B)

    # -------------------------------------------------------------------------
    # 1. Guardar resumen global
    # -------------------------------------------------------------------------
    resumenes = [r for r in (res_a, res_b) if r is not None]
    if resumenes:
        resumen_df = spark.createDataFrame(
            resumenes,
            ["periodo_feed", "n_nodos", "n_aristas", "n_componentes", "grado_promedio", "max_pagerank"],
        )
        resumen_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/g_grafos_resumen")
        print("\n[J8] Resumen topológico global:")
        resumen_df.show(truncate=False)

    # Si falta alguno de los dos períodos, terminar sin romper
    if nodos_a is None or nodos_b is None:
        print("[J8] WARN: Se requieren ambos períodos con datos para generar tablas comparativas.")
        spark.stop()
        return

    # -------------------------------------------------------------------------
    # 2. Comparación de nodos entre A y B
    # -------------------------------------------------------------------------
    nodos_comp = (
        nodos_a.alias("a")
        .join(nodos_b.alias("b"), "id", "full_outer")
        .select(
            F.col("id").alias("stop_id"),
            F.coalesce(F.col("b.stop_name"), F.col("a.stop_name")).alias("stop_name"),
            F.coalesce(F.col("b.stop_lat"), F.col("a.stop_lat")).alias("stop_lat"),
            F.coalesce(F.col("b.stop_lon"), F.col("a.stop_lon")).alias("stop_lon"),
            F.col("a.in_degree").alias("in_degree_a"),
            F.col("b.in_degree").alias("in_degree_b"),
            F.col("a.out_degree").alias("out_degree_a"),
            F.col("b.out_degree").alias("out_degree_b"),
            F.col("a.degree").alias("degree_a"),
            F.col("b.degree").alias("degree_b"),
            F.col("a.pagerank").alias("pagerank_a"),
            F.col("b.pagerank").alias("pagerank_b"),
            F.col("a.component").alias("component_a"),
            F.col("b.component").alias("component_b"),
        )
        .withColumn(
            "delta_degree",
            F.coalesce(F.col("degree_b"), F.lit(0)) - F.coalesce(F.col("degree_a"), F.lit(0)),
        )
        .withColumn(
            "delta_pagerank",
            F.coalesce(F.col("pagerank_b"), F.lit(0.0)) - F.coalesce(F.col("pagerank_a"), F.lit(0.0)),
        )
        .withColumn(
            "estado",
            F.when(F.col("degree_a").isNull(), F.lit("NUEVO"))
            .when(F.col("degree_b").isNull(), F.lit("ELIMINADO"))
            .when((F.col("delta_degree") != 0), F.lit("MODIFICADO"))
            .otherwise(F.lit("SIN_CAMBIO")),
        )
    )

    nodos_comp.write.mode("overwrite").parquet(f"{GOLD_DIR}/g_grafos_nodos_cambios")
    print("[J8] Top 10 paraderos por PageRank en Período B:")
    nodos_comp.orderBy(F.desc("pagerank_b")).select(
        "stop_id", "stop_name", "degree_a", "degree_b", "pagerank_b", "delta_pagerank", "estado"
    ).show(10, truncate=False)

    # -------------------------------------------------------------------------
    # 3. Comparación de aristas (tramos src -> dst) entre A y B
    # -------------------------------------------------------------------------
    edges_comp = (
        edges_a.alias("a")
        .join(edges_b.alias("b"), ["src", "dst"], "full_outer")
        .select(
            F.col("src"),
            F.col("dst"),
            F.col("a.n_trips").alias("n_trips_a"),
            F.col("b.n_trips").alias("n_trips_b"),
            F.col("a.n_routes").alias("n_routes_a"),
            F.col("b.n_routes").alias("n_routes_b"),
        )
        .withColumn(
            "delta_trips",
            F.coalesce(F.col("n_trips_b"), F.lit(0)) - F.coalesce(F.col("n_trips_a"), F.lit(0)),
        )
        .withColumn(
            "estado",
            F.when(F.col("n_trips_a").isNull(), F.lit("NUEVA"))
            .when(F.col("n_trips_b").isNull(), F.lit("ELIMINADA"))
            .when(F.col("delta_trips") != 0, F.lit("MODIFICADA"))
            .otherwise(F.lit("SIN_CAMBIO")),
        )
    )

    edges_comp.write.mode("overwrite").parquet(f"{GOLD_DIR}/g_grafos_aristas_cambios")
    print("[J8] Resumen de cambios en aristas:")
    edges_comp.groupBy("estado").count().show()

    print(f"[J8] Resultados guardados en: {GOLD_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
