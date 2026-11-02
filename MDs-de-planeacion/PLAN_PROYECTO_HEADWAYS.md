# Proyecto 1 – Análisis de Calidad de Frecuencias y "Headways" (Intervalos de Servicio)

> Documento de plan de acción para el equipo. Contiene el análisis del dataset,
> la información faltante completada y el plan paso a paso para implementar el
> proyecto con Apache Spark.

## 1. Contexto

- **Tecnología declarada:** Apache Spark (consistente con el Laboratorio 5, donde se usó Spark Core / RDD y se extendió con agregaciones por clave).
- **Tipo de proyecto:** Análisis de la regularidad del servicio de transporte público.
- **Objetivo:** calcular la **regularidad del servicio** en **paradas clave** a lo largo del día, mediante el análisis de **headways** (intervalos de tiempo entre la llegada/salida de buses consecutivos).

### Idea del proyecto (del enunciado del equipo / profe)

> Calcular la regularidad del servicio en paradas clave a lo largo del día.

### Preguntas que el proyecto debe responder (lo que se quiere analizar)

1. ¿Cuál es la distribución de headways (intervalos) en distintas paradas?
2. ¿En qué medida el servicio presenta **bunching** (irregularidad / buses consecutivos muy cercanos)?
3. ¿Cómo varía la regularidad entre **horas punta** y **horas valle**?

---

## 2. Análisis del dataset

### 2.1 Ubicación y fuente

- **Dataset:** Feed de datos de movilidad urbana (GTFS – Buses Red Metropolitana).
- **Carpeta:** `/home/pablo/Universidad/DCC/2do Año/Prosasamiento masivo de dartos/Proyecto-Grupo5/dataset_buses_rm 2/dataset_buses_rm/`
- **Nombre/ID del feed:** `mdb-3357-202607130343` (feed original) → dataset unificado `buses_rm`.
- **Publisher:** Red Metropolitana de Movilidad (RM) – filtrado para buses urbanos (`agency_id = 'RM'`, `route_type = 3`); se excluyeron Metro, EFE y BAA.
- **Período del feed:** dos ventanas (`periodo_feed`):
  - `20260829` (V167): vigencia real 29 ago – 2 oct 2026 (feriado: Fiesta Patrias, 18-19 sept).
  - `20261003` (V168): vigencia real 3 oct – 31 dic 2026 (feriados: 12, 31 oct, 8 y 25 dic).

### 2.2 Archivos del feed y tamaño aproximado

| Archivo | Contenido | Filas `20260829` | Filas `20261003` | **Total Unificado** | Llave de Cruce Principal | Descripción |
|---|---:|---:|---:|---:|---|---|
| `stop_times_buses_rm.csv` | Secuencia de paradas y tiempos relativos | 849,835 | 851,079 | **1,700,914** | `(periodo_feed, trip_id, stop_sequence)` | Viajes × paradas. |
| `trips_buses_rm.csv` | Viajes programados/plantilla | 14,464 | 14,486 | **28,950** | `(periodo_feed, trip_id)` | Vincula viaje → ruta, sentido, día, shape. |
| `frequencies_buses_rm.csv` | Franja horaria + headway | 14,464 | 14,486 | **28,950** | `(periodo_feed, trip_id)` | 1:1 con trips. |
| `routes_buses_rm.csv` | Catálogo de recorridos bus | 417 | 418 | **835** | `(periodo_feed, route_id)` | Bus = `route_type 3`. |
| `stops_buses_rm.csv` | Paraderos | 12,135 | 12,145 | **24,280** | `(periodo_feed, stop_id)` | Nombre + coordenadas. |
| `shapes_buses_rm.csv` | Trazados GPS | 408,349 | 408,606 | **816,955** | `(periodo_feed, shape_id, shape_pt_sequence)` | Opcional. |
| `calendar_buses_rm.csv` | Calendario por tipo de día | 3 | 3 | **6** | `(periodo_feed, service_id)` | L = Laboral, S = Sábado, D = Domingo. |
| `calendar_dates_buses_rm.csv` | Feriados/excepciones | 12 | 8 | **20** | `(periodo_feed, service_id, date)` | 1 = activar, 2 = desactivar. |

### 2.3 Esquemas relevantes (columnas)

- **`trips_buses_rm.csv`:** `route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, trip_short_name, wheelchair_accessible, bikes_allowed, periodo_feed`
- **`stop_times_buses_rm.csv`:** `trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, timepoint, periodo_feed`
- **`frequencies_buses_rm.csv`:** `trip_id, start_time, end_time, headway_secs, exact_times, periodo_feed`
- **`calendar_buses_rm.csv`:** `service_id, monday…sunday, start_date, end_date, periodo_feed`
- **`calendar_dates_buses_rm.csv`:** `service_id, date, exception_type, periodo_feed`
- **`routes_buses_rm.csv`:** `route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color, periodo_feed`
- **`stops_buses_rm.csv`:** `stop_id, stop_code, stop_name, stop_lat, stop_lon, stop_url, wheelchair_boarding, location_type, parent_station, level_id, periodo_feed`

### 2.4 Hechos observados / particularidades del dataset

1. **Feed tipo GTFS** unificado para buses RM (excluye Metro/EF E/BAA).
2. **Modelo por frecuencias (headways)**, no por horarios fijos (ver sección 7).
3. **`stop_times_buses_rm.csv`** es la tabla central: una fila = un viaje (`trip_id`) recorriendo una parada (`stop_id`), con `arrival_time`/`departure_time` **relativos** al inicio del viaje y `stop_sequence`.
4. **Formato de hora no estrictamente GTFS en algunos registros:** horas con un dígito (ej. `0:00:00`); hay que normalizar a segundos totales.
5. **`frequencies.txt`:** headways programados por trip (`headway_secs`, rango ~200–19,800 s, promedio ≈1269 s), `exact_times = 0`.
6. **Viajes:** ~28,950 trips unificados; `direction_id` ∈ {0, 1} (0 = Ida, 1 = Regreso).
7. **Rutas:** buses RM (principalmente `route_type = 3`); ej. L1 (2,698 viajes en el feed plano), etc.

---

## 3. Información que faltaba y que ahora se completa

| Pregunta inicial | Respuesta / estado |
|---|---|
| ¿Formato del dataset? | **GTFS unificado** (CSV con header, separador `,`). |
| ¿Dónde está? | `Proyecto-Grupo5/dataset_buses_rm 2/dataset_buses_rm/` (unificado) y `mdb-3357-202607130343/` (feed original/plano). |
| ¿Granularidad? | Una fila de `stop_times` = (viaje × parada). |
| ¿Clave de agrupación? | `(stop_id, route_id)` — sugerida por el enunciado. |
| ¿Valor/métrica? | Headway entre consecutivos (`lag`). |
| ¿Dato programado? | `frequencies.headway_secs`. |
| ¿Tecnología? | Apache Spark. |

---

## 4. Plan de acción detallado (para todo el equipo)

### 4.1 Objetivo del pipeline

1. Unir `trips`, `stop_times` y `calendar` (y `frequencies`) para trabajar sobre un **día laboral tipo**.
2. Usar **ventana de Spark**: `lag("departure_time").over(Window.partitionBy("stop_id", "route_id").orderBy("departure_time"))` → calcular **headway** entre buses consecutivos.
3. Detectar **irregularidad / bunching** (headway real vs. headway programado, o umbral).
4. Comparar **horas punta vs. horas valle**.

### 4.2 Diseño del flujo de datos (ETL con Spark)

```
stop_times ──┐
             ├──▶ join trips (periodo_feed, trip_id) ──▶ join calendar (periodo_feed, service_id) ──▶ filter día laboral
             │
frequencies ─┘            (headway programado por trip)
        │
        └──▶ join frequencies (periodo_feed, trip_id) ──▶ (headway_scheduled)
```

1. **Ingesta:** leer los CSVs con `spark.read.option("header", True).option("delimiter", ",").csv(...)`.
2. **Join:** siempre con `periodo_feed` en la llave compuesta (`on=["periodo_feed", "trip_id"]`, etc.).
3. **Filtrado:** aplicar calendario (día laboral tipo → `service_id = "L"`) o fecha objetivo.
4. **Transformación:** convertir tiempos relativos a **absolutos**: hora_absoluta = `start_time(frequencies) + tiempo_relativo(stop_times)`.
5. **Ventana:** `Window.partitionBy("stop_id", "route_id").orderBy("departure_time")` → `lag` → headway real.
6. **Join con programación:** `frequencies.headway_secs` → headway programado.
7. **Irregularidad (bunching):** headway real < headway programado (o < umbral) → evento.
8. **Pico vs. valle:** agrupar por franja horaria y por parada/ruta.
9. **Output:** guardar resultados (`saveAsTextFile` / parquet).

### 4.3 Pasos de implementación sugeridos (PySpark, siguiendo el patrón del Lab 5)

```python
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import lag, col, split, udf, hour

spark = SparkSession.builder.appName("Headways").getOrCreate()

trips   = spark.read.option("header", True).option("delimiter", ",").csv(base + "/trips_buses_rm.csv")
st      = spark.read.option("header", True).option("delimiter", ",").csv(base + "/stop_times_buses_rm.csv")
cal     = spark.read.option("header", True).option("delimiter", ",").csv(base + "/calendar_buses_rm.csv")
freq    = spark.read.option("header", True).option("delimiter", ",").csv(base + "/frequencies_buses_rm.csv")

# join trips -> route_id ... (siempre con periodo_feed)
df = st.join(trips, ["periodo_feed", "trip_id"], "inner")

# día laboral tipo
df = df.join(cal, ["periodo_feed", "service_id"], "inner").filter(col("monday") == 1)

# absoluto = start_time + relativo
df = df.withColumn("dep_secs", ...)

w = Window.partitionBy("stop_id", "route_id").orderBy("departure_time")
df = df.withColumn("prev_dep", lag("dep_secs").over(w)).withColumn("headway", col("dep_secs") - col("prev_dep"))

df = df.join(freq, ["periodo_feed", "trip_id"], "left")

# irregularidad: headway real < headway programado
irregular = df.filter(col("headway").isNotNull() & (col("headway") < col("headway_secs")))

# pico vs valle: bucket por hora
...

result = irregular.groupBy("route_id", "stop_id", "hour").agg(...)
result.write.saveAsTextFile(...)
```

### 4.4 Validación

1. Empezar con un **viaje/parada/ruta de prueba** (ej. una terminal) y un subconjunto pequeño.
2. Verificar headway manualmente (diferencia entre `departure_time` consecutivos).
3. Progresivamente: toda la red.

### 4.5 Ejecución en cluster

- Python: `spark-submit --master spark://cluster-01:7077 Headways.py <in> <out>`
- Java 8/Scala: mismo patrón del Lab 5 (`mdp-spark.jar ...`).
- Output: `saveAsTextFile` → `part-*`.

### 4.6 Entregables

- Solución en Spark (Java 8 / Scala / Python) para headways.
- Resultado para al menos una parada clave / ruta de muestra.
- Comparación pico vs. valle.

### 4.7 División sugerida del trabajo

| Integrante | Responsabilidad |
|---|---|
| 1 | Ingesta GTFS + joins (trips, stop_times, calendar, frequencies). |
| 2 | Cálculo de headways con window (`lag`). |
| 3 | Detección de irregularidad (bunching) + agregaciones pico/valle. |
| 4 | Output, validación y reporte/documentación. |

---

## 5. Decisiones que el equipo debe tomar (preguntas abiertas)

1. **Paradas clave:** ¿qué paradas se consideran "clave"? (terminales, estaciones de intercambio, stops con mayor demanda).
2. **Día laboral tipo:** ¿`service_id = "L"` (lunes a viernes) o un día específico?
3. **Horas pico vs. valle:** ¿definición operativa? (ej. pico: 07:00–09:00 y 17:00–20:00).
4. **Umbral de bunching:** ¿headway real < headway programado? ¿o < X minutos/segundos?
5. **Métrica final:** avg/median/min/max headway por parada/ruta/hora; % de intervalos irregulares.

---

## 6. Referencias técnicas

- Laboratorio 5 (Apache Spark): promedio por clave, agregación con `aggregateByKey`, `saveAsTextFile`.
- Ventana functions en Spark: `Window.partitionBy(...).orderBy(...)` + `lag`.
- GTFS: campos `trip_id`, `arrival_time`, `departure_time`, `headway_secs`, `service_id`.

---

## 7. Aprendizajes recientes del dataset (README_2.md – guía de joins y diccionario GTFS)

El archivo `README_2.md` aportó precisiones clave sobre el dataset que deben incorporarse al plan:

1. **Dataset definitivo del proyecto:** GTFS unificado de buses RM (excluye Metro, EFE y BAA), con dos periodos:
   - `periodo_feed = 20260829` (V167): vigencia real 29 ago – 2 oct 2026 (feriado: Fiesta Patrias, 18-19 sept).
   - `periodo_feed = 20261003` (V168): vigencia real 3 oct – 31 dic 2026 (feriados: 12, 31 oct, 8 y 25 dic).

2. **Regla de oro en Spark:** TODO `JOIN`/`groupBy`/ventana debe incluir `periodo_feed` como llave compuesta (los IDs se repiten entre periodos → sin esto, duplicación de filas).

3. **Modelo del feed: por FRECUENCIAS (headways), no por horarios fijos:**
   - `stop_times`: `arrival_time`/`departure_time` son **tiempos relativos** desde el primer paradero del viaje (`0:00:00` en `stop_sequence=1`).
   - `frequencies`: ventana horaria real (`start_time`→`end_time`) + `headway_secs` (intervalo entre buses; buses/hora = `3600 / headway_secs`).
   - Hora absoluta en una parada = `start_time + tiempo_relativo` (puede superar `24:00:00`, p.ej. `25:15:00` = 01:15 AM del día siguiente).

4. **direction_id:** `0` = Ida (I), `1` = Regreso (R).

5. **Cuidados de joins:** no unir `shapes` (N:M con stop_times) directamente; `calendar_dates` se filtra por fecha antes de unir.
