"""
Fase final (Proyecto 2) - Generación de figuras a partir de las tablas Gold.

Lee los datasets Parquet de la Capa Gold (build/gold) usando Pandas y PyArrow,
y genera gráficos (.png) optimizados y sin errores visuales de formato.

Uso:
    python3 figures.py
"""

import os
from pathlib import Path
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# -----------------------------------------------------------------------------
# Configuración de Rutas y Estilos
# -----------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent

raw_gold = os.environ.get("GOLD_DIR", str(PROJECT_DIR / "build" / "gold"))
GOLD_DIR = raw_gold.replace("file://", "")
FIG_DIR = os.environ.get("FIG_DIR", str(PROJECT_DIR / "build" / "figures"))

FMT = ".png"
COMUNAS_PATH = os.environ.get(
    "COMUNAS_PATH", str(PROJECT_DIR / "data" / "comunas_rm.geojson")
)
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")


def fig_headway_changes():
    """Pregunta 3: Recorridos que cambiaron su frecuencia (Headways)."""
    path = os.path.join(GOLD_DIR, "headways")
    if not os.path.exists(path):
        print(f"[WARN] No existe {path}, omitiendo fig1.")
        return

    df = pd.read_parquet(path)
    if "periodo_feed" not in df.columns or "avg_headway_secs" not in df.columns:
        print("[WARN] Columnas necesarias no presentes en headways.")
        return

    pivot = df.pivot_table(
        index=["route_id", "direction_id"],
        columns="periodo_feed",
        values="avg_headway_secs",
        observed=False
    ).reset_index()

    periodos = [c for c in pivot.columns if c not in ["route_id", "direction_id"]]
    if len(periodos) >= 2:
        col_a, col_b = periodos[0], periodos[1]
        pivot["delta_headway"] = pivot[col_b] - pivot[col_a]

        pdf = pivot[pivot["delta_headway"].abs() > 60].sort_values("delta_headway", ascending=False).head(20)

        if not pdf.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            labels = pdf["route_id"].astype(str) + " (Dir: " + pdf["direction_id"].astype(str) + ")"
            colors = ["crimson" if x > 0 else "seagreen" for x in pdf["delta_headway"]]

            bars = ax.barh(labels, pdf["delta_headway"], color=colors, height=0.4)
            ax.set_xlabel("Delta Headway Promedio (B - A) [Segundos]")
            ax.set_ylabel("Recorrido (Dirección)")
            ax.set_title("Top 20 Variaciones de Frecuencia (Verde = Menos espera, Rojo = Mayor espera)")

            for bar in bars:
                width = bar.get_width()
                ax.text(
                    width / 2,
                    bar.get_y() + bar.get_height() / 2,
                    f"{int(width)}s",
                    va="center",
                    ha="center",
                    color="white",
                    fontsize=10,
                    fontweight="bold"
                )

            plt.tight_layout()
            plt.savefig(os.path.join(FIG_DIR, f"fig1_headway_changes{FMT}"), dpi=300)
            plt.close()
            print("[OK] Generada fig1_headway_changes.png")


def fig_delta_salidas_recorrido():
    """Pregunta 4/5: Comparativa de Oferta Total de Salidas Diarias (V167 vs V168)."""
    path = os.path.join(GOLD_DIR, "g_oferta_hora")
    if not os.path.exists(path):
        print(f"[WARN] No existe {path}, omitiendo fig2.")
        return

    df = pd.read_parquet(path)

    # Si 'hora' es NaN, sumamos las salidas por periodo_feed
    if "periodo_feed" in df.columns and "salidas_hora" in df.columns:
        # Agrupamos por periodo por si acaso hay múltiples filas
        resumen = df.groupby("periodo_feed")["salidas_hora"].sum().reset_index()

        if len(resumen) >= 2:
            fig, ax = plt.subplots(figsize=(8, 6))

            bars = ax.bar(
                resumen["periodo_feed"].astype(str),
                resumen["salidas_hora"],
                color=["royalblue", "darkorange"],
                width=0.5,
                edgecolor="black"
            )

            ax.set_xlabel("Periodo Feed (V167 vs V168)")
            ax.set_ylabel("Total de Salidas Diarias en la Red")
            ax.set_title("Comparativa de Oferta Total Diaria de Buses")

            # Anotar los valores sobre las barras
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    height + 100,
                    f"{int(height):,}",
                    ha="center",
                    va="bottom",
                    fontweight="bold"
                )

            # Ajustar límite Y para dar espacio a los textos
            ax.set_ylim(0, resumen["salidas_hora"].max() * 1.15)

            plt.tight_layout()
            plt.savefig(os.path.join(FIG_DIR, f"fig2_delta_salidas_recorrido{FMT}"), dpi=300)
            plt.close()
            print("[OK] Generada fig2_delta_salidas_recorrido.png (Comparativa Total Diaria)")
        else:
            print("[WARN] Se requieren al menos 2 periodos en g_oferta_hora para graficar.")
    else:
        print("[ERROR] Las columnas de g_oferta_hora no coinciden con las esperadas.")


def fig_delta_paraderos():
    """Preguntas 2 y 5: Impacto operacional y estado de paraderos."""
    path = os.path.join(GOLD_DIR, "g_stops_cambios")
    if not os.path.exists(path):
        print(f"[WARN] No existe {path}, omitiendo fig3.")
        return

    df = pd.read_parquet(path)
    if "estado" in df.columns and "stop_id" in df.columns:
        pdf = df[df["estado"] != "SIN_CAMBIO"]
        if not pdf.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            counts = pdf["estado"].value_counts()
            bars = ax.bar(counts.index.astype(str), counts.values, color="darkorange", width=0.4, edgecolor="black")

            ax.set_xlabel("Estado de Cambio en Paraderos")
            ax.set_ylabel("Cantidad de Paraderos Afectados")
            ax.set_title("Distribución de Paraderos Modificados / Eliminados")

            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    height + 0.05,
                    f"{int(height)}",
                    ha="center",
                    va="bottom",
                    fontweight="bold"
                )

            ax.set_ylim(0, max(counts.values) + 1)
            plt.tight_layout()
            plt.savefig(os.path.join(FIG_DIR, f"fig3_delta_paraderos{FMT}"), dpi=300)
            plt.close()
            print("[OK] Generada fig3_delta_paraderos.png")


def fig_counts_estados():
    """Preguntas 1 y 5: Conteo consolidado de cambios por entidad y estado."""
    path = os.path.join(GOLD_DIR, "g_changelog")
    if not os.path.exists(path):
        print(f"[WARN] No existe {path}, omitiendo fig4.")
        return

    pdf = pd.read_parquet(path)
    counts = pdf.groupby(["tipo_cambio", "estado"]).size().unstack(fill_value=0)

    fig, ax = plt.subplots(figsize=(10, 6))
    counts.plot(kind="barh", stacked=True, ax=ax, colormap="tab10")

    ax.set_xlabel("Cantidad de entidades afectadas")
    ax.set_ylabel("Tipo de Entidad")
    ax.set_title("Resumen General del Changelog Consolidado (V167 -> V168)")

    totals = counts.sum(axis=1)
    for i, total in enumerate(totals):
        ax.text(total + 0.05, i, str(int(total)), va="center", fontweight="bold")

    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"fig4_counts_estados{FMT}"), dpi=300)
    plt.close()
    print("[OK] Generada fig4_counts_estados.png")

def _detectar_col_comuna(gdf):
    """Detecta el nombre estándar de la columna de comuna (BCN / IDE Chile)."""
    candidatos = ["Comuna", "COMUNA", "comuna", "NOM_COMUNA", "nom_comuna", "NOM_COM"]
    for c in candidatos:
        if c in gdf.columns:
            return c
    raise KeyError(f"No se encontró columna de comuna en {list(gdf.columns)}")


def obtener_metricas_por_comuna():
    """Cruza g_grafos_nodos_cambios con polígonos de comunas y retorna (resumen_df, comunas_gdf)."""
    try:
        import geopandas as gpd
    except ImportError:
        print("[WARN] Falta geopandas (pip install geopandas), omitiendo análisis por comuna.")
        return None, None

    nodos_path = os.path.join(GOLD_DIR, "g_grafos_nodos_cambios")
    if not os.path.exists(nodos_path):
        print(f"[WARN] No existe {nodos_path}, omitiendo análisis por comuna.")
        return None, None

    if not os.path.exists(COMUNAS_PATH):
        print(f"[WARN] No existe archivo de comunas en {COMUNAS_PATH}. Define COMUNAS_PATH.")
        return None, None

    nodos = pd.read_parquet(nodos_path).dropna(subset=["stop_lat", "stop_lon"])
    if nodos.empty:
        print("[WARN] g_grafos_nodos_cambios no tiene coordenadas válidas.")
        return None, None

    gdf_nodos = gpd.GeoDataFrame(
        nodos,
        geometry=gpd.points_from_xy(nodos["stop_lon"], nodos["stop_lat"]),
        crs="EPSG:4326",
    )

    comunas = gpd.read_file(COMUNAS_PATH).to_crs("EPSG:4326")
    col_comuna = _detectar_col_comuna(comunas)
    comunas = comunas[[col_comuna, "geometry"]].rename(columns={col_comuna: "comuna"})

    joined = gpd.sjoin(gdf_nodos, comunas, how="left", predicate="within").drop(
        columns=["index_right"]
    )
    joined["comuna"] = joined["comuna"].fillna("Sin asignar")

    resumen = (
        joined.groupby("comuna", as_index=False)
        .agg(
            n_paraderos=("stop_id", "count"),
            grado_prom_b=("degree_b", "mean"),
            pagerank_prom_b=("pagerank_b", "mean"),
            pagerank_max_b=("pagerank_b", "max"),
            delta_degree_total=("delta_degree", "sum"),
            delta_pagerank_prom=("delta_pagerank", "mean"),
            paraderos_modificados=("estado", lambda s: (s != "SIN_CAMBIO").sum()),
        )
        .sort_values("pagerank_prom_b", ascending=False)
    )

    # Exportar tabla resumen en CSV para el informe
    csv_out = os.path.join(FIG_DIR, "tabla_grafos_comunas.csv")
    resumen.to_csv(csv_out, index=False)
    print(f"[OK] Generada {csv_out}")

    comunas_con_datos = comunas.merge(resumen, on="comuna", how="inner")
    return resumen, comunas_con_datos


def fig_grafos_comunas():
    """Genera panel de barras por comuna (fig5) y mapa coroplético estático (fig6)."""
    resumen, comunas_gdf = obtener_metricas_por_comuna()
    if resumen is None or resumen.empty:
        return

    df_validas = resumen[resumen["comuna"] != "Sin asignar"]
    if df_validas.empty:
        print("[WARN] Ningún paradero coincidió con los polígonos de comunas.")
        return

    # --- Fig 5: Panel doble (Top PageRank + Variación de conectividad) ---
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    top_pr = df_validas.nlargest(15, "pagerank_prom_b").sort_values("pagerank_prom_b")
    ax1.barh(top_pr["comuna"], top_pr["pagerank_prom_b"], color="steelblue", edgecolor="black")
    ax1.set_xlabel("PageRank Promedio (Período B)")
    ax1.set_title("Top 15 Comunas por Centralidad (PageRank)")
    for i, v in enumerate(top_pr["pagerank_prom_b"]):
        ax1.text(v + 0.01, i, f"{v:.2f}", va="center", fontsize=8)

    cambios = df_validas[df_validas["delta_degree_total"] != 0]
    if not cambios.empty:
        top_delta = pd.concat([
            cambios.nlargest(8, "delta_degree_total"),
            cambios.nsmallest(8, "delta_degree_total"),
        ]).drop_duplicates("comuna").sort_values("delta_degree_total")

        colors = ["seagreen" if x > 0 else "crimson" for x in top_delta["delta_degree_total"]]
        ax2.barh(top_delta["comuna"], top_delta["delta_degree_total"], color=colors, edgecolor="black")
        ax2.axvline(0, color="black", linewidth=0.8)
        ax2.set_xlabel("Variación Neta de Grado (B - A)")
        ax2.set_title("Comunas con Mayor Cambio de Conectividad")
    else:
        ax2.text(0.5, 0.5, "Sin cambios de grado entre períodos", ha="center", va="center")
        ax2.set_axis_off()

    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"fig5_grafos_comunas_barras{FMT}"), dpi=300)
    plt.close()
    print("[OK] Generada fig5_grafos_comunas_barras.png")

    # --- Fig 6: Mapa coroplético estático (sin librerías interactivas) ---
    if comunas_gdf is not None and not comunas_gdf.empty:
        fig, ax = plt.subplots(figsize=(9, 9))
        comunas_gdf.plot(
            column="pagerank_prom_b",
            cmap="YlOrRd",
            linewidth=0.6,
            edgecolor="0.3",
            legend=True,
            legend_kwds={"label": "PageRank Promedio por Comuna", "shrink": 0.7},
            ax=ax,
        )
        ax.set_title("Centralidad Promedio de la Red de Buses por Comuna (RM)")
        ax.set_axis_off()
        plt.tight_layout()
        plt.savefig(os.path.join(FIG_DIR, f"fig6_grafos_comunas_mapa{FMT}"), dpi=300)
        plt.close()
        print("[OK] Generada fig6_grafos_comunas_mapa.png")



def main():
    os.makedirs(FIG_DIR, exist_ok=True)
    print(f"[FASE FINAL] Generando figuras optimizadas en: {FIG_DIR}")
    fig_headway_changes()
    fig_delta_salidas_recorrido()
    fig_delta_paraderos()
    fig_counts_estados()
    fig_grafos_comunas()
    print(f"[FASE FINAL] ¡Proceso completado con éxito! Revisa '{FIG_DIR}'.")




if __name__ == "__main__":
    main()
