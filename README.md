Markdown

# Proyecto : Análisis de Evolución de la Red de Buses del Gran Santiago (GTFS V167 vs V168)

Este repositorio contiene el código y los scripts de procesamiento y generación de reportes finales para el análisis comparativo de la red de transporte público del Gran Santiago entre la versión de GTFS **V167 (`20260829`)** y **V168 (`20261003`)**.

---

## Estructura del Proyecto

```text
   proyecto2/
   ├── build/
   │   ├── gold/           # Tablas procesadas en capa Gold (Parquet)
   │   └── figures/        # Directorio de salida para los gráficos generados (.png)
   ├── figures.py          # Script de generación de gráficos optimizados
   ├── print_summary.py    # Script de reportes tabulares y texto detallado
   ├── requirements.txt    # Dependencias del entorno virtual para visualización
   └── README.md           # Documentación del proyecto
```

### 1. Ejecución de el programa para el procesado de los datos.

```text
export DATASET_DIR="file:///data/2026/uhadoop/psuarez/proyecto2/dataset_buses_rm/dataset_buses_rm"
export SILVER_DIR="file:///data/2026/uhadoop/psuarez/proyecto2/build/silver"
export GOLD_DIR="file:///data/2026/uhadoop/psuarez/proyecto2/build/gold"
export FIG_DIR="/data/2026/uhadoop/psuarez/proyecto2/build/figures"

spark-submit jobs/J0_ingest.py && \
spark-submit jobs/J1_routes.py && \
spark-submit jobs/J2_stops.py && \
spark-submit jobs/J3_headways.py && \
spark-submit jobs/J4_secuencias.py && \
spark-submit jobs/J5_impacto.py && \
spark-submit jobs/J5b_oferta_hora.py && \
spark-submit jobs/J6_shapes.py && \
spark-submit jobs/J7_changelog.py
```

### 2. Configuración del Entorno Virtual (venv) y Dependencias

Para la ejecución de los scripts de visualización y reportes (figures.py y print_summary.py), se requiere un entorno virtual de Python con las librerías de soporte (como pandas, pyarrow y matplotlib).

    Nota: El procesamiento pesado de datos (capas Silver a Gold) se realiza a través de Spark/Hadoop en el clúster. El entorno virtual es exclusivo para la capa de presentación y reporte final.

Pasos de instalación en el servidor/clúster:

    Crear el entorno virtual (si aún no está creado):
    Bash

    python3 -m venv venv_proyecto

    Activar el entorno virtual:
    Bash

    source venv_proyecto/bin/activate

    Instalar las dependencias (asegúrate de tener el archivo requirements.txt en la raíz):
    Bash

    pip install -r requirements.txt

### 3. Ejecución de Scripts

Una vez activo el entorno virtual venv_proyecto, puedes ejecutar los scripts de reporte y visualización de la siguiente manera:
A. Generación de Figuras y Gráficos (figures.py)

Este script lee los datasets Parquet de la capa Gold y genera automáticamente las figuras optimizadas en la carpeta build/figures/:
Bash

python3 figures.py

    Salidas generadas:

        fig1_headway_changes.png: Variaciones de frecuencia (headways) en servicios críticos.

        fig2_delta_salidas_recorrido.png: Comparativa de oferta total diaria de la red.

        fig3_delta_paraderos.png: Distribución de paraderos modificados y nuevos.

        fig4_counts_estados.png: Resumen general consolidado del changelog por entidad.