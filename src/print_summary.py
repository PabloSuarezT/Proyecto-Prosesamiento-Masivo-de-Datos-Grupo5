"""
Script de resumen detallado y filtrado (Proyecto 2) - Capa Gold.
Muestra únicamente variaciones reales y filtra atributos modificados.

Uso:
    python3 print_summary.py
"""

import os
from pathlib import Path
import pandas as pd

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

PROJECT_DIR = Path(__file__).resolve().parent
GOLD_DIR = os.environ.get("GOLD_DIR", str(PROJECT_DIR / "build" / "gold"))

def print_details():
    print("="*85)
    print("1. CAMBIOS REALES EN HEADWAYS (Delta != 0)")
    print("="*85)
    path = os.path.join(GOLD_DIR, "headways")
    if os.path.exists(path):
        df = pd.read_parquet(path)
        pivot = df.pivot_table(
            index=["route_id", "direction_id"], 
            columns="periodo_feed", 
            values="avg_headway_secs",
            observed=False
        ).reset_index()
        
        periodos = [c for c in pivot.columns if c not in ["route_id", "direction_id"]]
        if len(periodos) >= 2:
            pivot["delta_headway"] = pivot[periodos[1]] - pivot[periodos[0]]
            # Filtrar estrictamente solo los cambios distintos de 0
            cambios_headway = pivot[pivot["delta_headway"].abs() > 0.001]
            print(cambios_headway.to_string(index=False))
        else:
            print(df.head(15).to_string(index=False))
    else:
        print(f"[WARN] No se encontró el directorio {path}")

    print("\n" + "="*85)
    print("2. RESUMEN GENERAL / CHANGELOG (g_changelog)")
    print("="*85)
    path = os.path.join(GOLD_DIR, "g_changelog")
    if os.path.exists(path):
        df = pd.read_parquet(path)
        counts = df.groupby(["tipo_cambio", "estado"]).size().unstack(fill_value=0)
        print(counts.to_string())
    else:
        print(f"[WARN] No se encontró el directorio {path}")

    print("\n" + "="*85)
    print("3. DETALLE DE PARADEROS (g_stops_cambios)")
    print("="*85)
    path_stops = os.path.join(GOLD_DIR, "g_stops_cambios")
    if os.path.exists(path_stops):
        df = pd.read_parquet(path_stops)
        
        print("Conteo de estados en paraderos:")
        print(df["estado"].value_counts())
        print("-" * 50)

        # 3.1 Todos los paraderos NUEVOS (13 en total)
        print("\n[A] TODOS LOS PARADEROS NUEVOS (13 registros):")
        nuevos = df[df["estado"] == "NUEVO"]
        if not nuevos.empty:
            cols_nuevos = [c for c in ["stop_id", "stop_name_b", "stop_lat_b", "stop_lon_b", "stop_code_b"] if c in nuevos.columns]
            print(nuevos[cols_nuevos].to_string(index=False))
        else:
            print("No se encontraron paraderos nuevos.")

        # 3.2 Filtrar paraderos MODIFICADOS con cambios reales (movidos == True o cambio de nombre/código)
        print("\n[B] PARADEROS MODIFICADOS CON CAMBIOS EFECTIVOS (Movidos o con ajuste de nombre/código):")
        if {"movido", "stop_name_a", "stop_name_b"}.issubset(df.columns):
            # Criterio de modificación real: o se movieron espacialmente o el nombre cambió
            mod_reales = df[(df["estado"] == "MODIFICADO") & ((df["movido"] == True) | (df["stop_name_a"] != df["stop_name_b"]))]
            
            print(f"Total de paraderos con modificaciones efectivas detectadas: {len(mod_reales)}")
            if not mod_reales.empty:
                cols_mod = [c for c in ["stop_id", "stop_name_a", "stop_name_b", "movido", "stop_lat_a", "stop_lat_b"] if c in mod_reales.columns]
                print(mod_reales.head(15).to_string(index=False))
            else:
                print("No se encontraron paraderos modificados con cambios puramente espaciales o de nombre (todos corresponden a otros metadatos internos del GTFS).")
        else:
            print("Las columnas necesarias para filtrar no están presentes.")
    else:
        print(f"[WARN] No se encontró el directorio {path_stops}")

    print("\n" + "="*85)
    print("4. OFERTA HORARIA / TOTAL DIARIO (g_oferta_hora)")
    print("="*85)
    path = os.path.join(GOLD_DIR, "g_oferta_hora")
    if os.path.exists(path):
        df = pd.read_parquet(path)
        print(df.to_string(index=False))
    else:
        print(f"[WARN] No se encontró el directorio {path}")

if __name__ == "__main__":
    print_details()