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

- **Dataset:** Feed de datos de movilidad urbana.
- **Carpeta:** `/mnt/c/Users/pablo/Downloads/mdb-3357-202607130343/`
- **Nombre/ID del feed:** `mdb-3357-202607130343`
- **Publisher:** Red Metropolitana de Movilidad (RM), EFE Trenes de Chile, BAA.
- **Período del feed:** 2026-07-04 → 2026-12-31 (`feed_info`: start 20260704, end 20261231, version V166.20260704).

### 2.2 Archivos del feed y tamaño aproximado

| Archivo | Contenido | Filas (aprox.) | Tamaño aprox. |
|---|---|---|---|
| `agency.txt` | Agencias (RM, EFE, BAA, Metro) | 4 | 582 B |
| `calendar.txt` | Calendario de servicios (días de semana) | 6 | 300 B |
| `calendar_dates.txt` | Excepciones/fechas especiales | 24 | 368 B |
| `feed_info.txt` | Metadatos del feed | 1 | 165 B |
| `frequencies.txt` | Headways programados por trip | 14,520 | 544 KB |
| `levels.txt` | Niveles de estaciones | 474 | 15 KB |
| `pathways.txt` | Rutas/peajes entre paradas | 7,388 | 404 KB |
| `routes.txt` | Catálogo de rutas | 427 | 26 KB |
| `shapes.txt` | Geometría de rutas | 456,334 | 14.7 MB |
| `stop_times.txt` | **Viajes × paradas con horarios** | ~1,097,121 | **50.9 MB** |
| `trips.txt` | Catálogo de viajes | 26,137 | 1.5 MB |
| `stops.txt` | Catálogo de paradas | 18,442 | 1.4 MB |

**Total de filas de operación relevantes:** ~1.1M filas en `stop_times` (la más grande), con ~26k viajes.

### 2.3 Esquemas relevantes (columnas)

- **`trips.txt`:** `route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, trip_short_name, wheelchair_accessible, bikes_allowed`
- **`stop_times.txt`:** `trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, timepoint`
- **`frequencies.txt`:** `trip_id, start_time, end_time, headway_secs, exact_times`
- **`calendar.txt`:** `service_id, monday…sunday, start_date, end_date`
- **`calendar_dates.txt`:** `service_id, date, exception_type`
- **`routes.txt`:** `route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, ...`
- **`stops.txt`:** `stop_id, stop_code, stop_name, stop_lat, stop_lon, ...`

### 2.4 Hechos observados / particularidades del dataset

1. **Feed tipo GTFS** (formato estándar de transporte público: trips, stop_times, frequencies, calendar).
2. **Período:** 2026 (julio–diciembre). Servicios activos tipo `L` (lunes a viernes) para un "día laboral típico".
3. **`stop_times.txt`** es la tabla central: una fila = un viaje (`trip_id`) recorriendo una parada (`stop_id`), con `arrival_time` y `departure_time`, y `stop_sequence`.
4. **Formato de hora NO estrictamente GTFS en algunos registros:** se observan horas con un dígito (ej. `0:00:00` en vez de `00:00:00`). Hay que normalizar antes de hacer aritmética (convertir a segundos totales).
5. **`frequencies.txt`:** headways programados por trip (`headway_secs`, rango aproximado 200–19,800 s, promedio ≈1269 s), con `exact_times = 0` (schedules basados en headway).
6. **Viajes:** ~26,137 trips; `direction_id` ∈ {0, 1}; ~7,917 con servicio tipo `L` (laborable). Servicios distintos: L, S, D, F, LJ, V.
7. **Rutas:** 427 rutas (418 de tipo bus=3, 7 de tipo metro=1, 2 de otro tipo). Algunas rutas con muchos viajes: L1 (2,698), L5 (1,964), L2 (1,928), L4 (1,738), L3 (1,129), L4A (1,090), L6 (981). Rutas "metro" tipo MTR (~89 viajes), J08, G08, F20, E03, etc.

---

## 3. Información que faltaba y que ahora se completa

Con el dataset GTFS en mano, las brechas de información para el proyecto quedaron respondidas:

| Pregunta inicial | Respuesta / estado |
|---|---|
| ¿Formato del dataset? | **GTFS** (CSV/TSV con header, separador `,`; sin comillas complicadas). |
| ¿Dónde está? | `/mnt/c/Users/pablo/Downloads/mdb-3357-202607130343/` |
| ¿Granularidad? | Una fila de `stop_times` = (viaje × parada). |
| ¿Clave de agrupación? | `(stop_id, route_id)` — combinación sugerida por el enunciado (ventana por parada y ruta). |
| ¿Valor/métrica? | `departure_time` → headway entre consecutivos (`lag`). |
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
             ├──▶ join trips (trip_id) ──▶ join calendar (service_id) ──▶ filter día laboral
             │
frequencies ─┘            (headway programado por trip)
        │
        └──▶ join frequencies (trip_id) ──▶ (headway_scheduled)
```

1. **Ingesta:** leer `stop_times`, `trips`, `calendar`, `frequencies` con `spark.read.option("header", True).option("delimiter", ",").csv(...)`.
2. **Join:** `stop_times ⋈ trips` por `trip_id` → obtener `route_id`, `service_id`, `direction_id`, `stop_id`, `departure_time`, `stop_sequence`.
3. **Filtrado:** aplicar calendario (día laboral tipo → `service_id = "L"`) o fecha objetivo.
4. **Transformación:** normalizar `departure_time` `H:MM:SS`/`HH:MM:SS` → segundos acumulados. Crear ventana:
   `Window.partitionBy("stop_id", "route_id").orderBy("departure_time")`
   → `lag` → headway real = `departure_time - lag(departure_time)`.
5. **Join con programación:** `frequencies.headway_secs` → headway programado.
6. **Irregularidad (bunching):** headway real < headway programado (o < umbral definido) → evento de irregularidad.
7. **Pico vs. valle:** agrupar por franja horaria (`hour = floor(headway_seconds / 3600)` o hora de la parada) y por parada/ruta.
8. **Output:** guardar resultados (`saveAsTextFile` / parquet).

### 4.3 Pasos de implementación sugeridos (PySpark, siguiendo el patrón del Lab 5)

```python
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import lag, col, split, udf, hour

spark = SparkSession.builder.appName("Headways").getOrCreate()

trips   = spark.read.option("header", True).option("delimiter", ",").csv(base + "/trips.txt")
st      = spark.read.option("header", True).option("delimiter", ",").csv(base + "/stop_times.txt")
cal     = spark.read.option("header", True).option("delimiter", ",").csv(base + "/calendar.txt")
freq    = spark.read.option("header", True).option("delimiter", ",").csv(base + "/frequencies.txt")

# join trips -> route_id ...
df = st.join(trips, st.trip_id == trips.trip_id, "inner")

# día laboral tipo
df = df.join(cal, df.service_id == cal.service_id, "inner").filter(col("monday") == 1)

# minutos/segundos desde medianoche
df = df.withColumn("dep_secs", 
    (split(col("departure_time"), ":").getItem(0).cast("int") * 3600 +
     split(col("departure_time"), ":").getItem(1).cast("int") * 60 +
     split(col("departure_time"), ":").getItem(2).cast("int")))

w = Window.partitionBy("stop_id", "route_id").orderBy("departure_time")
df = df.withColumn("prev_dep", lag("dep_secs").over(w)).withColumn("headway", col("dep_secs") - col("prev_dep"))

df = df.join(freq, df.trip_id == freq.trip_id, "left")

# irregularidad: headway real < headway programado
irregular = df.filter(col("headway").isNotNull() & (col("headway") < col("headway_secs")))

# pico vs valle: bucket por hora
df = df.withColumn("hour", hour(col("departure_time").cast("timestamp")))

result = irregular.groupBy("route_id", "stop_id", "hour").agg(...)
result.write.saveAsTextFile(...)
```

> Nota: ajustar sintaxis según la versión de Spark del cluster (2.x/3.x) y al lenguaje elegido por el grupo (Java 8, Scala o Python).

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
