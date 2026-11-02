"""Fase final (Proyecto 2) - Generacion de figuras a partir de las tablas Gold.

Lee las tablas Gold del comparativo V167 -> V168 y genera figuras (PNG) con matplotlib.

Figuras:
    1. Headways modificados (delta headway A -> B) por trip.
    2. Delta de salidas/dia por recorrido (route x direction x service).
    3. Delta de buses/dia por paradero (top ganan / top pierden).
    4. Conteos de cambios por entidad y estado.

Uso:
    python3 generar_figuras.py
"""

import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType

PROJECT_DIR = Path(
    "/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5"
)
GOLD_DIR = Path(os.environ.get("GOLD_DIR", str(PROJECT_DIR / "gold" / "comparacion")))
OUT_DIR = Path(os.environ.get("FIG_DIR", str(PROJECT_DIR / "figures")))

FMT = ".png"


def _spark():
    return SparkSession.builder.appName("figuras-buses-rm").config(
        "spark.sql.shuffle.partitions", "200"
    ).getOrCreate()


def fig_headway_changes():
    df = (
        _spark()
        .read.parquet(str(GOLD_DIR / "g_headway_cambios"))
        .filter(col("estado") == lit("MODIFICADO"))
        .select(col("trip_id"), col("headway_a").alias("headway_a"), col("headway_b").alias("headway_b"))
    )
    pdf = df.toPandas().sort_values("headway_b", ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["trip_id"].astype(str), (pdf["headway_b"] - pdf["headway_a"]).astype(float), color="steelblue")
    ax.set_xlabel("Delta headway (B - A) en segundos")
    ax.set_title("Headways modificados entre V167 (20260829) y V168 (20261003)")
    plt.tight_layout()
    plt.savefig(str(OUT_DIR / f"fig1_headway_changes{FMT}"))
    plt.close()


def fig_delta_salidas_recorrido():
    df = (
        _spark()
        .read.parquet(str(GOLD_DIR / "g_recorrido_delta"))
        .filter(col("salidas_a").isNotNull())
        .select("route_id", "direction_id", "service_id", "salidas_a", "salidas_b", "delta_salidas")
    )
    pdf = df.toPandas().sort_values("delta_salidas", ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["route_id"].astype(str), pdf["delta_salidas"].astype(float), color="seagreen")
    ax.set_xlabel("Delta salidas/dia (B - A)")
    ax.set_title("Cambios en salidas por dia entre V167 y V168")
    plt.tight_layout()
    plt.savefig(str(OUT_DIR / f"fig2_delta_salidas_recorrido{FMT}"))
    plt.close()


def fig_delta_paraderos():
    df = (
        _spark()
        .read.parquet(str(GOLD_DIR / "g_delta_paraderos"))
        .filter(col("buses_dia_a").isNotNull())
        .select("service_id", "stop_id", "buses_dia_a", "buses_dia_b", "delta_buses_dia")
    )
    pdf = df.toPandas().sort_values("delta_buses_dia", ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["stop_id"].astype(str), pdf["delta_buses_dia"].astype(float), color="crimson")
    ax.set_xlabel("Delta buses/dia (B - A)")
    ax.set_title("Impacto en paraderos entre V167 y V168")
    plt.tight_layout()
    plt.savefig(str(OUT_DIR / f"fig3_delta_paraderos{FMT}"))
    plt.close()


def fig_counts_estados():
    frames = []
    specs = [
        ("route", "g_routes_cambios", "route_id"),
        ("stop", "g_stops_cambios", "stop_id"),
        ("trip_headway", "g_headway_cambios", "trip_id"),
        ("shape", "g_shapes_cambios", "shape_id"),
    ]
    for entidad, tabla, id_col in specs:
        df = (
            _spark()
            .read.parquet(str(GOLD_DIR / tabla))
            .filter(col("estado") != lit("SIN_CAMBIO"))
            .select(lit(entidad).alias("entidad"), col(id_col).alias("entidad_id"), col("estado"))
        )
        frames.append(df.toPandas())
    import pandas as pd

    pdf = pd.concat(frames, ignore_index=True)
    counts = pdf.groupby(["entidad", "estado"]).size().reset_index(name="n")
    fig, ax = plt.subplots(figsize=(10, 6))
    for estado, grp in counts.groupby("estado"):
        ax.barh(grp["entidad"].astype(str), grp["n"].astype(float), label=estado)
    ax.set_xlabel("Cantidad de cambios")
    ax.set_title("Cambios detectados por entidad y estado (V167 -> V168)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(str(OUT_DIR / f"fig4_counts_estados{FMT}"))
    plt.close()


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig_headway_changes()
    fig_delta_salidas_recorrido()
    fig_delta_paraderos()
    fig_counts_estados()
    print("[FASE FINAL] Figuras generadas en", OUT_DIR)


if __name__ == "__main__":
    main()
