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


def main():
    os.makedirs(FIG_DIR, exist_ok=True)
    print(f"[FASE FINAL] Generando figuras optimizadas en: {FIG_DIR}")
    fig_headway_changes()
    fig_delta_salidas_recorrido()
    fig_delta_paraderos()
    fig_counts_estados()
    print(f"[FASE FINAL] ¡Proceso completado con éxito! Revisa '{FIG_DIR}'.")


if __name__ == "__main__":
    main()