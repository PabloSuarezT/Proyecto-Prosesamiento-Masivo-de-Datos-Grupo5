# Proyecto 2 — Comparación de la red de buses RM (V167 → V168)

**Objetivo:** comparar automáticamente las dos versiones del GTFS unificado de buses RM
(`periodo_feed` A = `20260829` / V167, B = `20261003` / V168) y generar un changelog +
tablas Gold con los cambios.

**Tecnología:** Apache Spark (PySpark, `pyspark==2.1.0` — mismo que el lab), jobs vía `spark-submit`.

## Jobs implementados

| Job | Archivo | Gold generado |
|---|---|---|
| J0 | `jobs/J0_ingest.py` | `silver/buses_rm/*.parquet` (partition por `periodo_feed`) |
| J1 | `jobs/J1_routes.py` | `gold/comparacion/g_routes_cambios` |
| J2 | `jobs/J2_stops.py` | `gold/comparacion/g_stops_cambios` |
| J3 | `jobs/J3_headways.py` | `gold/comparacion/g_headway_cambios`, `gold/comparacion/g_recorrido_delta` |
| J5 | `jobs/J5_impacto.py` | `gold/comparacion/g_delta_paraderos` |
| J5b | `jobs/J5b_oferta_hora.py` | `gold/comparacion/g_delta_oferta_hora` |
| J6 | `jobs/J6_shapes.py` | `gold/comparacion/g_shapes_cambios` |
| J7 | `jobs/J7_changelog.py` | `gold/comparacion/g_changelog` |

Pendiente: **J4/J4b** — secuencias de viajes y tramos (Window/array).

## Variables de entorno

- `DATASET_DIR` → dataset de entrada (default: `dataset_buses_rm 2/dataset_buses_rm`).
- `SILVER_DIR` → capa Silver de salida (default: `silver/buses_rm`).
- `GOLD_DIR` → capa Gold de salida (default: `gold/comparacion`).

## Ejecución (en servidor)

```bash
export DATASET_DIR=/tmp/opencode/gtfs_test     # dataset de prueba
export SILVER_DIR=/tmp/opencode/silver_test
export GOLD_DIR=/tmp/opencode/gold_test

spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J0_ingest.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J1_routes.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J2_stops.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J3_headways.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J5_impacto.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J5b_oferta_hora.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J6_shapes.py
spark-submit --master <cluster> --py-files <deps> \
    proyecto2/jobs/J7_changelog.py
```

## Métricas / conceptos clave

- Headway programado: `frequencies.headway_secs`.
- Salidas por trip: `n = floor((fin - ini - 1) / headway) + 1` (end exclusivo).
- Delta de salidas por recorrido: suma de `n_salidas` por (route, direction, service).
- Delta de buses/día por paradero: Σ `n_salidas` de viajes que pasan el paradero.
- Delta de buses/hora: salidas × stop_times → pasadas → hora → count.
