#!/usr/bin/env python3
"""
Consultas de verificación para responder las 6 preguntas del Objetivo (Proyecto GTFS Buses RM V167->V168).

Lee tablas Gold y Silver ya procesadas en el clúster y genera salidas detalladas para:
  Q1. Rutas agregadas/eliminadas
  Q2. Paraderos agregados/eliminados/movidos/renombrados
  Q3. Trips que modificaron su frecuencia entre periodos
  Q4. Cambios de salidas/día por recorrido (ruta x dirección x servicio)
  Q5. Impacto en oferta por paradero (buses/día) y por hora
  Q6. Traducción de cambios de headway a flota en circulación (estimación/proxy)

Uso (ejemplos):
  spark-submit src/print_summary.py
  python3 src/print_summary.py

Variables de entorno:
  GOLD_DIR   -> ruta a capa Gold (default: <PROJECT_DIR>/build/gold)
  SILVER_DIR -> ruta a capa Silver (default: <PROJECT_DIR>/build/silver)
  PERIODO_A  -> periodo base (default: 20260829)
  PERIODO_B  -> periodo comparación (default: 20261003)
  SHOW_ROWS  -> máximo de filas a mostrar por tabla (default: 200)
  CHECKPOINT_DIR -> directorio para checkpoints (default: /tmp/print_summary_ckpt_$USER)
"""

import os
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

PROJECT_DIR = Path(__file__).resolve().parent
GOLD_DIR = os.environ.get("GOLD_DIR", str(PROJECT_DIR / "build" / "gold"))
SILVER_DIR = os.environ.get("SILVER_DIR", str(PROJECT_DIR / "build" / "silver"))
PERIODO_A = os.environ.get("PERIODO_A", "20260829")
PERIODO_B = os.environ.get("PERIODO_B", "20261003")
SHOW_ROWS = int(os.environ.get("SHOW_ROWS", "200"))
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", f"/tmp/print_summary_ckpt_{os.environ.get('USER','u')}_{uuid.uuid4().hex[:8]}")


def h(title):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def sub(title):
    print("\n" + "-" * 100)
    print(title)
    print("-" * 100)


def show_df(df, n=None, truncate=False):
    n = SHOW_ROWS if n is None else n
    if df is None:
        print("[WARN] DataFrame None")
        return
    try:
        cnt = df.count()
    except Exception as e:
        print(f"[WARN] No se pudo contar: {e}")
        cnt = None
    if cnt is not None:
        print(f"Filas: {cnt}")
    df.show(n, truncate=truncate)


def exists_path(spark, path):
    try:
        jvm = spark._jvm
        jsc = spark._jsc
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(p.toUri(), conf)
        return fs.exists(p)
    except Exception:
        return os.path.exists(path.replace("file://", ""))


def read_parquet_if_exists(spark, path, label=None):
    label = label or path
    try:
        if not exists_path(spark, path):
            print(f"[WARN] No existe: {path}")
            return None
    except Exception:
        pass
    try:
        df = spark.read.parquet(path)
        print(f"[OK] Leído: {label} -> {path}")
        return df
    except Exception as e:
        print(f"[WARN] No se pudo leer {label} ({path}): {e}")
        return None


def safe_checkpoint(df, name="df"):
    """Corta el linaje lógico para evitar StackOverflow en planes complejos."""
    if df is None:
        return None
    try:
        return df.localCheckpoint(eager=True)
    except Exception:
        try:
            df = df.cache()
            df.count()
            return df
        except Exception as e:
            print(f"[WARN] No se pudo materializar {name}: {e}")
            return df


def run_section(name, fn):
    try:
        fn()
    except Exception as e:
        print(f"\n[ERROR] Falló la sección {name}: {e}")
        import traceback
        traceback.print_exc()


def time_to_secs_expr(colname):
    """
    Extrae HH:MM:SS desde string/timestamp (p.ej. '08:30:00' o '2026-11-18 08:30:00')
    y convierte a segundos desde inicio de día (soporta horas >= 24).
    """
    s = F.col(colname).cast("string")
    # tomar último token separado por espacio/T y extraer patrón de hora
    base = F.regexp_extract(s, r"(\d{1,2}:\d{2}:\d{2})(?:\.\d+)?$", 1)
    base = F.when(base == "", F.regexp_extract(s, r"(\d{1,2}:\d{2}:\d{2})", 1)).otherwise(base)
    parts = F.split(base, ":")
    return F.when(
        base == "",
        F.lit(None).cast("long")
    ).otherwise(
        parts.getItem(0).cast("long") * 3600
        + F.coalesce(parts.getItem(1).cast("long"), F.lit(0))
        + F.coalesce(parts.getItem(2).cast("long"), F.lit(0))
    )


def time_str_expr(colname):
    s = F.col(colname).cast("string")
    base = F.regexp_extract(s, r"(\d{1,2}:\d{2}:\d{2})(?:\.\d+)?$", 1)
    return F.when(base == "", F.regexp_extract(s, r"(\d{1,2}:\d{2}:\d{2})", 1)).otherwise(base)


def ensure_time_cols(df, base_col):
    secs_col = f"{base_col}_secs"
    str_col = f"{base_col}_hhmmss"
    if base_col in df.columns:
        df = df.withColumn(str_col, time_str_expr(base_col))
    else:
        df = df.withColumn(str_col, F.lit(None).cast("string"))
    if secs_col in df.columns:
        df = df.withColumn(secs_col, F.col(secs_col).cast("long"))
        # si *_secs es nulo pero hay hora parseable, recalcular
        df = df.withColumn(secs_col, F.coalesce(F.col(secs_col), time_to_secs_expr(base_col))) if base_col in df.columns else df
    else:
        df = df.withColumn(secs_col, time_to_secs_expr(base_col) if base_col in df.columns else F.lit(None).cast("long"))
    return df


def normalize_periodo(df):
    if df is not None and "periodo_feed" in df.columns:
        return df.withColumn("periodo_feed", F.col("periodo_feed").cast("string"))
    return df


def q1_routes(spark):
    h("Q1. RUTAS AGREGADAS O ELIMINADAS (V167 -> V168)")
    df = read_parquet_if_exists(spark, f"{GOLD_DIR}/g_routes_cambios", "g_routes_cambios")
    if df is None:
        return

    sub("Conteo por estado")
    show_df(df.groupBy("estado").count().orderBy("estado"), n=50)

    cols = [c for c in [
        "route_id", "estado",
        "route_short_name_a", "route_short_name_b",
        "route_long_name_a", "route_long_name_b",
        "route_desc_a", "route_desc_b",
        "route_type_a", "route_type_b",
        "route_color_a", "route_color_b",
        "route_text_color_a", "route_text_color_b",
        "route_url_a", "route_url_b",
    ] if c in df.columns]

    sub("Listado de rutas NUEVAS")
    show_df(df.filter(F.col("estado") == "NUEVO").select(*cols).orderBy("route_id"))

    sub("Listado de rutas ELIMINADAS")
    show_df(df.filter(F.col("estado") == "ELIMINADO").select(*cols).orderBy("route_id"))

    sub("Listado de rutas MODIFICADAS (detalle, muestra)")
    show_df(df.filter(F.col("estado") == "MODIFICADO").select(*cols).orderBy("route_id"), n=min(SHOW_ROWS, 100))


def q2_stops(spark):
    h("Q2. PARADEROS AGREGADOS, ELIMINADOS, MOVIDOS O RENOMBRADOS")
    df = read_parquet_if_exists(spark, f"{GOLD_DIR}/g_stops_cambios", "g_stops_cambios")
    if df is None:
        return

    sub("Conteo por estado")
    show_df(df.groupBy("estado").count().orderBy("estado"), n=50)

    cols = [c for c in [
        "stop_id", "estado", "movido",
        "stop_name_a", "stop_name_b",
        "stop_lat_a", "stop_lat_b",
        "stop_lon_a", "stop_lon_b",
        "stop_code_a", "stop_code_b",
        "location_type_a", "location_type_b",
    ] if c in df.columns]
    base = df.select(*cols)

    sub("Paraderos NUEVOS (agregados)")
    show_df(base.filter(F.col("estado") == "NUEVO").orderBy("stop_id"))

    sub("Paraderos ELIMINADOS")
    show_df(base.filter(F.col("estado") == "ELIMINADO").orderBy("stop_id"))

    rename_cond = F.lit(False)
    if "stop_name_a" in df.columns and "stop_name_b" in df.columns:
        rename_cond = (F.col("stop_name_a").isNotNull() & F.col("stop_name_b").isNotNull() & (F.col("stop_name_a") != F.col("stop_name_b")))

    latlon_change_cond = F.lit(False)
    if set(["stop_lat_a", "stop_lat_b", "stop_lon_a", "stop_lon_b"]).issubset(df.columns):
        latlon_change_cond = (
            F.col("stop_lat_a").isNotNull() & F.col("stop_lat_b").isNotNull() &
            F.col("stop_lon_a").isNotNull() & F.col("stop_lon_b").isNotNull() &
            ((F.col("stop_lat_a") != F.col("stop_lat_b")) | (F.col("stop_lon_a") != F.col("stop_lon_b")))
        )

    movido_flag_cond = F.col("movido") == F.lit(True) if "movido" in df.columns else F.lit(False)

    clasif = base.withColumn("renombrado_nombre", rename_cond) \
                 .withColumn("cambio_latlon", latlon_change_cond) \
                 .withColumn("movido_flag", movido_flag_cond)

    sub("Conteos de paraderos movidos/renombrados (criterios explícitos)")
    show_df(clasif.agg(
        F.sum(F.when(F.col("movido_flag"), 1).otherwise(0)).alias("movido_segun_flag"),
        F.sum(F.when(F.col("cambio_latlon"), 1).otherwise(0)).alias("con_cambio_latlon"),
        F.sum(F.when(F.col("renombrado_nombre"), 1).otherwise(0)).alias("con_cambio_nombre"),
        F.sum(F.when(F.col("renombrado_nombre") & ~F.col("cambio_latlon"), 1).otherwise(0)).alias("renombrado_sin_cambio_latlon"),
        F.sum(F.when(F.col("renombrado_nombre") & F.col("cambio_latlon"), 1).otherwise(0)).alias("renombrado_y_cambio_latlon"),
        F.sum(F.when(F.col("cambio_latlon") & ~F.col("renombrado_nombre"), 1).otherwise(0)).alias("movido_sin_cambio_nombre"),
    ), n=10)

    sub("Paraderos RENOMBRADOS (cambio de stop_name entre A y B)")
    show_df(clasif.filter(F.col("renombrado_nombre")).select(*[c for c in cols if c in clasif.columns], "cambio_latlon", "movido_flag").orderBy("stop_id"))

    sub("Paraderos MOVIDOS segun flag 'movido'")
    show_df(clasif.filter(F.col("movido_flag")).select(*[c for c in cols if c in clasif.columns], "renombrado_nombre", "cambio_latlon").orderBy("stop_id"), n=min(SHOW_ROWS, 100))

    sub("Paraderos con cambio de lat/lon observado")
    show_df(clasif.filter(F.col("cambio_latlon")).select(*[c for c in cols if c in clasif.columns], "renombrado_nombre", "movido_flag").orderBy("stop_id"), n=min(SHOW_ROWS, 100))


def read_frequencies_with_secs(spark):
    df = read_parquet_if_exists(spark, f"{SILVER_DIR}/frequencies", "silver/frequencies")
    if df is None:
        return None
    df = normalize_periodo(df)
    df = ensure_time_cols(df, "start_time")
    df = ensure_time_cols(df, "end_time")
    if "headway_secs" in df.columns:
        df = df.withColumn("headway_secs", F.col("headway_secs").cast("long"))
    else:
        df = df.withColumn("headway_secs", F.lit(None).cast("long"))
    df = df.withColumn(
        "n_salidas",
        F.when(
            F.col("headway_secs").isNotNull() & (F.col("headway_secs") > 0) &
            F.col("start_time_secs").isNotNull() & F.col("end_time_secs").isNotNull() &
            (F.col("end_time_secs") > F.col("start_time_secs")),
            F.floor((F.col("end_time_secs") - F.col("start_time_secs") - 1) / F.col("headway_secs")) + 1
        ).otherwise(F.lit(0).cast("long"))
    )
    return safe_checkpoint(df, "frequencies_with_secs")


def read_trips(spark):
    df = read_parquet_if_exists(spark, f"{SILVER_DIR}/trips", "silver/trips")
    if df is None:
        return None
    return safe_checkpoint(normalize_periodo(df), "trips")


def q3_trips_frecuencia(spark, freq, trips):
    h("Q3. TRIPS QUE MODIFICARON SU FRECUENCIA ENTRE PERIODOS")
    if freq is None:
        print("[WARN] No se puede responder Q3 sin silver/frequencies")
        return

    sel = [c for c in ["periodo_feed", "trip_id", "start_time_hhmmss", "end_time_hhmmss", "start_time_secs", "end_time_secs", "headway_secs", "n_salidas"] if c in freq.columns]
    fA = freq.filter(F.col("periodo_feed") == PERIODO_A).select(*sel)
    fB = freq.filter(F.col("periodo_feed") == PERIODO_B).select(*sel)

    # Definición de frecuencia por trip: conjunto ordenado de (start,end,headway,n_salidas)
    def def_by_trip(df, suffix):
        d = df.withColumn(
            "def_item",
            F.concat_ws("|",
                F.coalesce(F.col("start_time_hhmmss"), F.lit("")),
                F.coalesce(F.col("end_time_hhmmss"), F.lit("")),
                F.coalesce(F.col("headway_secs").cast("string"), F.lit("")),
                F.coalesce(F.col("n_salidas").cast("string"), F.lit(""))
            )
        )
        agg = d.groupBy("trip_id").agg(
            F.sort_array(F.collect_set("def_item")).alias(f"defs_{suffix}"),
            F.sum(F.coalesce(F.col("n_salidas"), F.lit(0))).alias(f"salidas_total_{suffix}"),
            F.count(F.lit(1)).alias(f"filas_{suffix}")
        )
        return safe_checkpoint(agg, f"defs_{suffix}")

    da = def_by_trip(fA, "a")
    db = def_by_trip(fB, "b")
    cmp_trip = da.join(db, "trip_id", "full_outer")
    cmp_trip = cmp_trip.withColumn(
        "estado_trip",
        F.when(F.col("defs_a").isNull(), F.lit("SOLO_EN_B"))
         .when(F.col("defs_b").isNull(), F.lit("SOLO_EN_A"))
         .when(F.col("defs_a") == F.col("defs_b"), F.lit("SIN_CAMBIO"))
         .otherwise(F.lit("FRECUENCIA_MODIFICADA"))
    ).withColumn("delta_salidas_total", F.coalesce(F.col("salidas_total_b"), F.lit(0)) - F.coalesce(F.col("salidas_total_a"), F.lit(0)))
    cmp_trip = safe_checkpoint(cmp_trip, "cmp_trip")

    sub("Conteo por estado a nivel trip_id (definiciones de frecuencia)")
    show_df(cmp_trip.groupBy("estado_trip").count().orderBy("estado_trip"), n=50)

    sub("Resumen de salidas totales (según frecuencias) en trips con cambios/presencia parcial")
    show_df(cmp_trip.filter(F.col("estado_trip") != "SIN_CAMBIO").agg(
        F.count(F.lit(1)).alias("trips_afectados"),
        F.sum(F.when(F.col("estado_trip") == "FRECUENCIA_MODIFICADA", 1).otherwise(0)).alias("trips_modificados"),
        F.sum(F.when(F.col("estado_trip") == "SOLO_EN_A", 1).otherwise(0)).alias("solo_en_a"),
        F.sum(F.when(F.col("estado_trip") == "SOLO_EN_B", 1).otherwise(0)).alias("solo_en_b"),
        F.sum(F.coalesce(F.col("salidas_total_a"), F.lit(0))).alias("salidas_a_afectados"),
        F.sum(F.coalesce(F.col("salidas_total_b"), F.lit(0))).alias("salidas_b_afectados"),
        F.sum(F.col("delta_salidas_total")).alias("delta_salidas_afectados"),
    ), n=10)

    # Detalle enriquecido
    detalle = cmp_trip.filter(F.col("estado_trip") != "SIN_CAMBIO")
    if trips is not None:
        wanted = [c for c in ["trip_id", "route_id", "direction_id", "service_id", "shape_id"] if c in trips.columns]
        tA = trips.filter(F.col("periodo_feed") == PERIODO_A).select(*wanted)
        tB = trips.filter(F.col("periodo_feed") == PERIODO_B).select(*wanted)
        for c in wanted:
            if c != "trip_id":
                tA = tA.withColumnRenamed(c, f"{c}_a_trip")
                tB = tB.withColumnRenamed(c, f"{c}_b_trip")
        detalle = detalle.join(tA, on="trip_id", how="left").join(tB, on="trip_id", how="left")
        for c in ["route_id", "direction_id", "service_id", "shape_id"]:
            ca, cb = f"{c}_a_trip", f"{c}_b_trip"
            if ca in detalle.columns or cb in detalle.columns:
                expr_a = F.col(ca) if ca in detalle.columns else F.lit(None)
                expr_b = F.col(cb) if cb in detalle.columns else F.lit(None)
                detalle = detalle.withColumn(c, F.coalesce(expr_b, expr_a))

    show_cols = [c for c in [
        "trip_id", "route_id", "direction_id", "service_id", "estado_trip",
        "filas_a", "filas_b", "salidas_total_a", "salidas_total_b", "delta_salidas_total",
        "defs_a", "defs_b"
    ] if c in detalle.columns]

    sub("Detalle de trips con frecuencia modificada o presentes solo en un periodo (muestra)")
    show_df(detalle.select(*show_cols).orderBy(F.abs(F.coalesce(F.col("delta_salidas_total"), F.lit(0))).desc(), "trip_id"), n=min(SHOW_ROWS, 100))

    sub("Trips NUEVOS en frecuencias (SOLO_EN_B)")
    show_df(detalle.filter(F.col("estado_trip") == "SOLO_EN_B").select(*[c for c in show_cols if c in detalle.columns]).orderBy("trip_id"), n=min(SHOW_ROWS, 100))

    sub("Trips ELIMINADOS de frecuencias (SOLO_EN_A)")
    show_df(detalle.filter(F.col("estado_trip") == "SOLO_EN_A").select(*[c for c in show_cols if c in detalle.columns]).orderBy("trip_id"), n=min(SHOW_ROWS, 100))

    sub("Trips con FRECUENCIA_MODIFICADA (misma identidad de trip, distinta definición)")
    show_df(detalle.filter(F.col("estado_trip") == "FRECUENCIA_MODIFICADA").select(*[c for c in show_cols if c in detalle.columns]).orderBy(F.abs(F.coalesce(F.col("delta_salidas_total"), F.lit(0))).desc(), "trip_id"), n=min(SHOW_ROWS, 100))


def q4_salidas_recorrido(spark, freq, trips):
    h("Q4. CAMBIOS DE SALIDAS/DIA POR RECORRIDO (ruta x direccion x servicio)")
    if freq is None or trips is None:
        print("[WARN] No se puede responder Q4 sin silver/frequencies y silver/trips")
        return

    tcols = [c for c in ["periodo_feed", "trip_id", "route_id", "direction_id", "service_id"] if c in trips.columns]
    joined = freq.select("periodo_feed", "trip_id", "n_salidas").join(trips.select(*tcols), on=["periodo_feed", "trip_id"], how="left")
    if "route_id" not in joined.columns:
        print("[WARN] trips no contiene route_id; no se puede agrupar por recorrido")
        return

    group_keys = [c for c in ["route_id", "direction_id", "service_id"] if c in joined.columns]
    agg = joined.groupBy("periodo_feed", *group_keys).agg(
        F.sum(F.coalesce(F.col("n_salidas"), F.lit(0))).alias("salidas_dia"),
        F.countDistinct("trip_id").alias("trips_distintos")
    )

    # Pivot por periodo para evitar joins de alias que profundizan el plan
    val = F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("salidas_dia")).otherwise(F.lit(0))).alias("salidas_dia_a")
    valb = F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("salidas_dia")).otherwise(F.lit(0))).alias("salidas_dia_b")
    ta = F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("trips_distintos")).otherwise(F.lit(0))).alias("trips_distintos_a")
    tb = F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("trips_distintos")).otherwise(F.lit(0))).alias("trips_distintos_b")
    cmp_ab = agg.groupBy(*group_keys).agg(val, valb, ta, tb)
    cmp_ab = cmp_ab.withColumn("delta_salidas_dia", F.col("salidas_dia_b") - F.col("salidas_dia_a"))
    cmp_ab = safe_checkpoint(cmp_ab, "cmp_salidas_recorrido")

    sub("Conteo de recorridos por tipo de cambio en salidas/dia")
    show_df(cmp_ab.agg(
        F.count(F.lit(1)).alias("total_recorridos"),
        F.sum(F.when(F.col("delta_salidas_dia") > 0, 1).otherwise(0)).alias("aumentan"),
        F.sum(F.when(F.col("delta_salidas_dia") < 0, 1).otherwise(0)).alias("disminuyen"),
        F.sum(F.when(F.col("delta_salidas_dia") == 0, 1).otherwise(0)).alias("sin_cambio"),
        F.sum(F.when((F.col("salidas_dia_a") == 0) & (F.col("salidas_dia_b") > 0), 1).otherwise(0)).alias("solo_en_b"),
        F.sum(F.when((F.col("salidas_dia_a") > 0) & (F.col("salidas_dia_b") == 0), 1).otherwise(0)).alias("solo_en_a"),
        F.sum(F.col("salidas_dia_a")).alias("total_salidas_a"),
        F.sum(F.col("salidas_dia_b")).alias("total_salidas_b"),
        F.sum(F.col("delta_salidas_dia")).alias("delta_total"),
    ), n=20)

    sub("Recorridos con delta_salidas_dia != 0 (ordenado por |delta| desc)")
    show_df(cmp_ab.filter(F.col("delta_salidas_dia") != 0).orderBy(F.abs(F.col("delta_salidas_dia")).desc(), *group_keys))

    sub("Top 50 aumentos de salidas/dia")
    show_df(cmp_ab.filter(F.col("delta_salidas_dia") > 0).orderBy(F.col("delta_salidas_dia").desc(), *group_keys), n=50)

    sub("Top 50 disminuciones de salidas/dia")
    show_df(cmp_ab.filter(F.col("delta_salidas_dia") < 0).orderBy(F.col("delta_salidas_dia").asc(), *group_keys), n=50)


def q5_oferta(spark, freq, trips):
    h("Q5. IMPACTO EN OFERTA POR PARADERO (buses/dia) Y POR HORA")

    sub("Q5.1 Oferta por hora (g_oferta_hora, Gold)")
    oferta_hora = read_parquet_if_exists(spark, f"{GOLD_DIR}/g_oferta_hora", "g_oferta_hora")
    if oferta_hora is not None:
        oferta_hora = normalize_periodo(oferta_hora)
        if "hora" in oferta_hora.columns:
            cmp_h = oferta_hora.groupBy(F.col("hora").cast("int").alias("hora")).agg(
                F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("salidas_hora")).otherwise(F.lit(0))).alias("salidas_hora_a"),
                F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("salidas_hora")).otherwise(F.lit(0))).alias("salidas_hora_b"),
            ).withColumn("delta_salidas_hora", F.col("salidas_hora_b") - F.col("salidas_hora_a")).orderBy("hora")
            show_df(cmp_h, n=48)
            sub("Totales diarios (suma sobre horas disponibles en g_oferta_hora)")
            show_df(cmp_h.agg(
                F.sum("salidas_hora_a").alias("total_a"),
                F.sum("salidas_hora_b").alias("total_b"),
                F.sum("delta_salidas_hora").alias("delta_total")
            ), n=10)
        else:
            show_df(oferta_hora)
    else:
        print("[WARN] Sin g_oferta_hora no se puede mostrar detalle por hora desde Gold")

    sub("Q5.2 Oferta por paradero (buses/dia estimados desde Silver: salidas de trips que sirven el paradero)")
    st = read_parquet_if_exists(spark, f"{SILVER_DIR}/stop_times", "silver/stop_times")
    stops = read_parquet_if_exists(spark, f"{SILVER_DIR}/stops", "silver/stops")
    if freq is None or trips is None or st is None:
        print("[WARN] No se puede calcular oferta por paradero sin silver/frequencies, silver/trips y silver/stop_times")
        return

    st = normalize_periodo(st)
    trip_salidas = freq.groupBy("periodo_feed", "trip_id").agg(F.sum(F.coalesce(F.col("n_salidas"), F.lit(0))).alias("n_salidas_trip"))
    st_pairs = st.select("periodo_feed", "trip_id", "stop_id").dropna(subset=["trip_id", "stop_id"]).dropDuplicates()
    st_pairs = safe_checkpoint(st_pairs, "st_pairs")
    sp = st_pairs.join(trip_salidas, on=["periodo_feed", "trip_id"], how="left").fillna({"n_salidas_trip": 0})

    by_stop = sp.groupBy("periodo_feed", "stop_id").agg(
        F.sum("n_salidas_trip").alias("buses_dia_paradero"),
        F.countDistinct("trip_id").alias("trips_que_pasan")
    )

    cmp_s = by_stop.groupBy("stop_id").agg(
        F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("buses_dia_paradero")).otherwise(F.lit(0))).alias("buses_dia_a"),
        F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("buses_dia_paradero")).otherwise(F.lit(0))).alias("buses_dia_b"),
        F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("trips_que_pasan")).otherwise(F.lit(0))).alias("trips_a"),
        F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("trips_que_pasan")).otherwise(F.lit(0))).alias("trips_b"),
    ).withColumn("delta_buses_dia", F.col("buses_dia_b") - F.col("buses_dia_a"))
    cmp_s = safe_checkpoint(cmp_s, "cmp_stops_oferta")

    if stops is not None:
        stops = normalize_periodo(stops)
        sA = stops.filter(F.col("periodo_feed") == PERIODO_A).select("stop_id", F.col("stop_name").alias("stop_name_a"))
        sB = stops.filter(F.col("periodo_feed") == PERIODO_B).select("stop_id", F.col("stop_name").alias("stop_name_b"))
        cmp_s = cmp_s.join(sB, "stop_id", "left").join(sA, "stop_id", "left").withColumn("stop_name", F.coalesce(F.col("stop_name_b"), F.col("stop_name_a")))

    sub("Resumen impacto por paradero")
    show_df(cmp_s.agg(
        F.count(F.lit(1)).alias("paraderos_total"),
        F.sum(F.when(F.col("delta_buses_dia") > 0, 1).otherwise(0)).alias("con_aumento"),
        F.sum(F.when(F.col("delta_buses_dia") < 0, 1).otherwise(0)).alias("con_disminucion"),
        F.sum(F.when(F.col("delta_buses_dia") == 0, 1).otherwise(0)).alias("sin_cambio"),
        F.sum(F.when((F.col("buses_dia_a") == 0) & (F.col("buses_dia_b") > 0), 1).otherwise(0)).alias("solo_en_b"),
        F.sum(F.when((F.col("buses_dia_a") > 0) & (F.col("buses_dia_b") == 0), 1).otherwise(0)).alias("solo_en_a"),
    ), n=20)

    cols_show = [c for c in ["stop_id", "stop_name", "buses_dia_a", "buses_dia_b", "delta_buses_dia", "trips_a", "trips_b"] if c in cmp_s.columns]
    sub("Top 50 paraderos con mayor aumento de buses/dia")
    show_df(cmp_s.filter(F.col("delta_buses_dia") > 0).orderBy(F.col("delta_buses_dia").desc()).select(*cols_show), n=50)
    sub("Top 50 paraderos con mayor disminucion de buses/dia")
    show_df(cmp_s.filter(F.col("delta_buses_dia") < 0).orderBy(F.col("delta_buses_dia").asc()).select(*cols_show), n=50)


def q6_flota(spark, freq, trips):
    h("Q6. TRADUCCION DE CAMBIO DE HEADWAY A FLOTA EN CIRCULACION (ESTIMACION)")
    print("Nota: proxy de vehículos requeridos por intervalo/fila de frecuencia:")
    print("  vehiculos_requeridos ~ duracion_ciclo_trip / headway_secs")
    print("duracion_ciclo_trip se estima desde stop_times (max(time_secs)-min(time_secs)) por trip y periodo.")
    print("Es una estimación aproximada (supone ciclo ~ duración observada del trip), no medición directa de flota.")

    st = read_parquet_if_exists(spark, f"{SILVER_DIR}/stop_times", "silver/stop_times")
    if freq is None or trips is None or st is None:
        print("[WARN] No se puede estimar flota sin silver/frequencies, silver/trips y silver/stop_times")
        return

    st = normalize_periodo(st)
    st = ensure_time_cols(st, "arrival_time")
    st = ensure_time_cols(st, "departure_time")
    st = st.withColumn("time_secs", F.coalesce(F.col("departure_time_secs"), F.col("arrival_time_secs")))

    trip_dur = st.groupBy("periodo_feed", "trip_id").agg(
        (F.max("time_secs") - F.min("time_secs")).alias("duracion_trip_secs"),
        F.count(F.lit(1)).alias("registros_stop_times")
    ).filter(F.col("duracion_trip_secs").isNotNull() & (F.col("duracion_trip_secs") > 0))
    trip_dur = safe_checkpoint(trip_dur, "trip_dur")

    tcols = [c for c in ["periodo_feed", "trip_id", "route_id", "direction_id", "service_id"] if c in trips.columns]
    f = freq.select(*[c for c in ["periodo_feed", "trip_id", "headway_secs", "n_salidas"] if c in freq.columns])
    base = f.join(trip_dur, on=["periodo_feed", "trip_id"], how="inner").join(trips.select(*tcols), on=["periodo_feed", "trip_id"], how="left")
    base = base.withColumn(
        "vehiculos_requeridos",
        F.when(F.col("headway_secs").isNotNull() & (F.col("headway_secs") > 0), F.col("duracion_trip_secs") / F.col("headway_secs"))
    )

    group_keys = [c for c in ["route_id", "direction_id", "service_id"] if c in base.columns]
    agg = base.groupBy("periodo_feed", *group_keys).agg(
        F.sum("vehiculos_requeridos").alias("vehiculos_proxy_suma"),
        F.avg("vehiculos_requeridos").alias("vehiculos_proxy_promedio"),
        F.avg("headway_secs").alias("avg_headway_secs"),
        F.avg("duracion_trip_secs").alias("avg_duracion_trip_secs"),
        F.sum(F.coalesce(F.col("n_salidas"), F.lit(0))).alias("salidas_dia"),
        F.countDistinct("trip_id").alias("trips_distintos")
    )
    agg = safe_checkpoint(agg, "agg_flota")

    sub("Resumen global del proxy de flota por periodo")
    show_df(agg.groupBy("periodo_feed").agg(
        F.sum("vehiculos_proxy_suma").alias("vehiculos_proxy_suma_total"),
        F.avg("vehiculos_proxy_promedio").alias("vehiculos_proxy_promedio_global"),
        F.sum("salidas_dia").alias("salidas_dia_total"),
        F.avg("avg_headway_secs").alias("avg_headway_secs_global")
    ).orderBy("periodo_feed"), n=10)

    cmp_ab = agg.groupBy(*group_keys).agg(
        F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("vehiculos_proxy_suma")).otherwise(F.lit(0.0))).alias("vehiculos_proxy_suma_a"),
        F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("vehiculos_proxy_suma")).otherwise(F.lit(0.0))).alias("vehiculos_proxy_suma_b"),
        F.avg(F.when(F.col("periodo_feed") == PERIODO_A, F.col("avg_headway_secs"))).alias("avg_headway_secs_a"),
        F.avg(F.when(F.col("periodo_feed") == PERIODO_B, F.col("avg_headway_secs"))).alias("avg_headway_secs_b"),
        F.sum(F.when(F.col("periodo_feed") == PERIODO_A, F.col("salidas_dia")).otherwise(F.lit(0))).alias("salidas_dia_a"),
        F.sum(F.when(F.col("periodo_feed") == PERIODO_B, F.col("salidas_dia")).otherwise(F.lit(0))).alias("salidas_dia_b"),
    ).withColumn("delta_vehiculos_proxy_suma", F.col("vehiculos_proxy_suma_b") - F.col("vehiculos_proxy_suma_a")) \
     .withColumn("delta_avg_headway_secs", F.coalesce(F.col("avg_headway_secs_b"), F.lit(0.0)) - F.coalesce(F.col("avg_headway_secs_a"), F.lit(0.0)))
    cmp_ab = safe_checkpoint(cmp_ab, "cmp_flota")

    sub("Recorridos con mayor cambio absoluto en proxy de flota (y su delta de headway)")
    show_df(cmp_ab.orderBy(F.abs(F.col("delta_vehiculos_proxy_suma")).desc(), *group_keys), n=100)

    sub("Ejemplos donde baja headway (delta negativo) y cambia proxy de flota")
    show_df(cmp_ab.filter(F.col("delta_avg_headway_secs") < 0).orderBy(F.col("delta_avg_headway_secs").asc(), *group_keys), n=50)


def resumen_base(spark):
    h("RESUMEN BASE (contexto, similar a reporte actual)")
    dfc = read_parquet_if_exists(spark, f"{GOLD_DIR}/g_changelog", "g_changelog")
    if dfc is not None and set(["tipo_cambio", "estado"]).issubset(dfc.columns):
        sub("Conteo g_changelog por tipo_cambio y estado")
        show_df(dfc.groupBy("tipo_cambio", "estado").count().orderBy("tipo_cambio", "estado"), n=100)

    hw = read_parquet_if_exists(spark, f"{GOLD_DIR}/headways", "gold/headways")
    if hw is not None and set(["route_id", "direction_id", "periodo_feed", "avg_headway_secs"]).issubset(hw.columns):
        sub("Cambios reales en headways promedio por route_id,direction_id (Gold/headways)")
        a = hw.filter(F.col("periodo_feed") == PERIODO_A).select("route_id", "direction_id", F.col("avg_headway_secs").alias("hw_a"))
        b = hw.filter(F.col("periodo_feed") == PERIODO_B).select("route_id", "direction_id", F.col("avg_headway_secs").alias("hw_b"))
        cmp_hw = a.join(b, ["route_id", "direction_id"], "full_outer").select(
            F.coalesce(a.route_id, b.route_id).alias("route_id"),
            F.coalesce(a.direction_id, b.direction_id).alias("direction_id"),
            F.col("hw_a"), F.col("hw_b"),
            (F.col("hw_b") - F.col("hw_a")).alias("delta_headway")
        ).filter(F.col("delta_headway").isNotNull() & (F.abs(F.col("delta_headway")) > 0.001)).orderBy(F.abs(F.col("delta_headway")).desc())
        show_df(cmp_hw, n=100)


def main():
    spark = (
        SparkSession.builder
        .appName("consultas-informe-buses-rm")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)
        print(f"Checkpoint dir: {CHECKPOINT_DIR}")
    except Exception as e:
        print(f"[WARN] No se pudo configurar checkpoint dir ({CHECKPOINT_DIR}): {e}")

    print("Configuración:")
    print(f"  GOLD_DIR   = {GOLD_DIR}")
    print(f"  SILVER_DIR = {SILVER_DIR}")
    print(f"  PERIODO_A  = {PERIODO_A}")
    print(f"  PERIODO_B  = {PERIODO_B}")
    print(f"  SHOW_ROWS  = {SHOW_ROWS}")

    run_section("resumen_base", lambda: resumen_base(spark))
    run_section("Q1", lambda: q1_routes(spark))
    run_section("Q2", lambda: q2_stops(spark))

    freq = read_frequencies_with_secs(spark)
    trips = read_trips(spark)

    run_section("Q3", lambda: q3_trips_frecuencia(spark, freq, trips))
    run_section("Q4", lambda: q4_salidas_recorrido(spark, freq, trips))
    run_section("Q5", lambda: q5_oferta(spark, freq, trips))
    run_section("Q6", lambda: q6_flota(spark, freq, trips))

    h("FIN DE CONSULTAS")
    spark.stop()


if __name__ == "__main__":
    main()

