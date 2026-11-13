# Proyecto 2 — Comparación de la red de buses RM (V167 → V168)

**Objetivo:** comparar automáticamente las dos versiones del GTFS unificado de buses RM
(`periodo_feed` A = `20260829` / V167, B = `20261003` / V168) y generar un changelog +
tablas Gold con los cambios.

**Tecnología:** Apache Spark (PySpark, `pyspark==2.1.0` — mismo que el lab), jobs vía `spark-submit`.

## Jobs

| Job | Archivo | Gold generado | Para qué sirve |
|---|---|---|---|
| J0 | `jobs/J0_ingest.py` | `silver/buses_rm/*.parquet` (partition por `periodo_feed`) | Ingesta CSV → capa Silver (Parquet particionado por `periodo_feed`), convierte tiempos a segundos. Base de todo. |
| J1 | `jobs/J1_routes.py` | `gold/comparacion/g_routes_cambios` | Compara routes A vs B (full outer por `route_id`) → NUEVO/ELIMINADO/MODIFICADO/SIN_CAMBIO. Ej: `B46` nuevo, 0 rutas modificadas. |
| J2 | `jobs/J2_stops.py` | `gold/comparacion/g_stops_cambios` | Compara stops A vs B (+ flag `movido`) → eliminados/nuevos/movidos/renombrados. Ej: `PA665`, `PA718`, `PA86` eliminados; 13 nuevos; 13 movidos; 6 renombrados. |
| J3 | `jobs/J3_headways.py` | `gold/comparacion/g_headway_cambios`, `gold/comparacion/g_recorrido_delta` | Compara headways A vs B por trip (salidas: `n = floor((fin - ini - 1)/headway) + 1`) → NUEVO/ELIMINADO/MODIFICADO/SIN_CAMBIO + delta salidas/día por recorrido. Ej: trip con headway 675→600 (MODIFICADO); 22 NUEVOS; 94 headway MODIFICADOS. |
| J4 | `jobs/J4_secuencias.py` | `gold/comparacion/g_secuencias_cambios` | Compara secuencias de paradas A vs B por `(trip_id, stop_sequence)` → NUEVO/ELIMINADO/MODIFICADO/SIN_CAMBIO. Ej: 106 combos recorrido-sentido-día con secuencia distinta. |
| J5 | `jobs/J5_impacto.py` | `gold/comparacion/g_delta_paraderos` | `buses/día` por `(service_id, stop_id)` = Σ `n_salidas` de viajes que pasan el paradero → full outer A vs B → delta. Ej: `PA299` −444 buses/día. |
| J5b | `jobs/J5b_oferta_hora.py` | `gold/comparacion/g_delta_oferta_hora` | Salidas (frequencies + k) → join stop_times → pasadas → hora → buses/hora → full outer A vs B → delta. |
| J6 | `jobs/J6_shapes.py` | `gold/comparacion/g_shapes_cambios` | Largo de shapes (Σ distancias euclidianas entre vértices consecutivos) A vs B. |
| J7 | `jobs/J7_changelog.py` | `gold/comparacion/g_changelog` | Consolida routes + stops + headways en changelog unificado. |

**Pendiente:** **J4b** — tramos de viajes (Window `lead`).

## Fase final (figures + informe)

- `figures.py` → genera figuras en `figures/`:
  - `fig1_headway_changes`: headways modificados (delta headway B−A) por trip.
  - `fig2_delta_salidas_recorrido`: delta salidas/día por recorrido.
  - `fig3_delta_paraderos`: delta buses/día por paradero.
  - `fig4_counts_estados`: cambios por entidad y estado.

## Variables de entorno

- `DATASET_DIR` → dataset de entrada (default: `dataset_buses_rm 2/dataset_buses_rm`).
- `SILVER_DIR` → capa Silver (default: `silver/buses_rm`).
- `GOLD_DIR` → capa Gold (default: `gold/comparacion`).
- `FIG_DIR` → carpeta de figuras (default: `figures`).

## Ejecución (en servidor)

```bash
export DATASET_DIR="/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5/proyecto2/test_data"   # dataset de prueba (carpeta dentro del proyecto)
export SILVER_DIR=/tmp/opencode/silver_test
export GOLD_DIR=/tmp/opencode/gold_test
export FIG_DIR=/tmp/opencode/figures_test

spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J0_ingest.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J1_routes.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J2_stops.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J3_headways.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J4_secuencias.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J5_impacto.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J5b_oferta_hora.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J6_shapes.py
spark-submit --master <cluster> --py-files <deps> proyecto2/jobs/J7_changelog.py
python3 generar_figuras.py

## con dataset real:
export DATASET_DIR="/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5/dataset_buses_rm 2/dataset_buses_rm"
```

## Métricas / conceptos clave

- Headway programado: `frequencies.headway_secs`.
- Salidas por trip: `n = floor((fin - ini - 1) / headway) + 1` (end exclusivo).
- Delta salidas/día por recorrido: Σ `n_salidas` por (route, direction, service).
- Delta buses/día por paradero: Σ `n_salidas` de viajes que pasan el paradero.
- Delta buses/hora: salidas × stop_times → pasadas → hora → count.

## Próximos pasos

- Redactar informe final.
- Ejecutar en servidor con dataset de prueba → corregir → probar con dataset real → subir.
