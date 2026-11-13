# Informe de Análisis Final: Evolución de la Red de Buses del Gran Santiago (V167 vs. V168)

Este documento presenta el análisis técnico y cuantitativo de la transición de la red de transporte público del Gran Santiago entre la versión **V167** (`20260829`) y la versión **V168** (`20261003`), procesado sobre una arquitectura distribuida (Spark/Hadoop - Capa Gold).

---

## 1. Resumen General del Changelog (`g_changelog`)

La tabla consolidada cuantifica las modificaciones estructurales en las entidades principales del GTFS:

| Tipo de Entidad (`tipo_cambio`) | ELIMINADO | MODIFICADO | NUEVO | Total Afectados |
| :--- | :---: | :---: | :---: | :---: |
| **Frecuencias / Headways (`headway`)** | 0 | 3 | 1 | **4** |
| **Rutas (`route`)** | 0 | 417 | 1 | **418** |
| **Paraderos (`stop`)** | 3 | 12,132 | 13 | **12,148** |

---

## 2. Variaciones Reales de Frecuencia (`headways` con $\Delta \neq 0$)

Se identificaron los servicios que modificaron sus intervalos de paso promedio (headway en segundos) entre ambos periodos:

| Recorrido (`route_id`) | Dirección (`direction_id`) | Headway V167 (s) | Headway V168 (s) | Delta Headway ($\Delta s$) |
| :---: | :---: | :---: | :---: | :---: |
| **203c** | 0 | 631.44 | 606.22 | -25.22 |
| **206** | 0 | 1,343.86 | 1,341.03 | -2.83 |
| **208** | 0 | 680.52 | 679.00 | -1.52 |
| **216** | 1 | 846.35 | 842.19 | -4.15 |
| **228** | 0 | 631.52 | 625.26 | -6.26 |
| **229** | 0 | 993.30 | 990.74 | -2.57 |
| **301** | 0 | 713.72 | 783.38 | +69.66 |
| **301** | 1 | 678.86 | 760.76 | +81.90 |
| **301c** | 0 | 793.81 | 707.86 | -85.95 |
| **301c** | 1 | 1,573.52 | 1,517.00 | -56.52 |
| **306** | 0 | 945.96 | 964.26 | +18.30 |
| **306** | 1 | 875.96 | 880.13 | +4.17 |
| **G23** | 0 | 863.00 | 855.52 | -7.48 |
| **G23** | 1 | 966.78 | 961.22 | -5.57 |

---

## 3. Estado y Catastro de Paraderos (`g_stops_cambios`)

### A. Paraderos Nuevos (13 registros incorporados)
| Stop ID | Nombre del Paradero (`stop_name_b`) | Latitud | Longitud |
| :--- | :--- | :---: | :---: |
| **PA736** | PA736-Bandera / esq. Agustinas | -33.441143 | -70.652040 |
| **PA75** | PA75-Bandera / esq. Catedral | -33.437838 | -70.652517 |
| **PA76** | PA76-Bandera / esq. Rosas | -33.435400 | -70.652880 |
| **PA769** | PA769-Parada / Instituto Nacional | -33.446950 | -70.651044 |
| **PB2249** | PB2249-Parada 4 / Recoleta Dominica | -33.422758 | -70.645194 |
| **PC1348** | PC1348-Av. La Dehesa / esq. Av. Las Condes | -33.368426 | -70.511729 |
| **PD1664** | PD1664-Exequiel Fernández / esq. Amador Neghme R. | -33.497857 | -70.602061 |
| **PF1021** | PF1021-Avenida Tobalaba / esq. Av. San Carlos | -33.591566 | -70.542431 |
| **PF1041** | PF1041-Av. Troncal Las Torres / esq. Av. Nueva Tobalaba | -33.593717 | -70.540946 |
| **PF847** | PF847-Parada 3 / Hospital El Peral | -33.579679 | -70.551777 |
| **PG2197** | PG2197-Av.José M. Balmaceda / esq. La Jardinera | -33.595918 | -70.670400 |
| **PH1652** | PH1652-Raúl Silva Henríquez / esq. Plano Regulador | -33.496023 | -70.686982 |
| **PH1668** | PH1668-Cooperación / esq. Cura Morales | -33.494520 | -70.683033 |

### B. Muestra de Paraderos Modificados con Cambios Efectivos (Geográficos o Nominales)
De un total de 12,132 registros marcados como modificados (principalmente metadatos internos), se identificaron aquellos con desplazamientos espaciales o ajustes de nomenclatura:

| Stop ID | Nombre Anterior (`stop_name_a`) | Nombre Nuevo (`stop_name_b`) | Movido | Latitud (A $\rightarrow$ B) |
| :--- | :--- | :--- | :---: | :---: |
| **PB1054** | Pedro Fontova / Las Puertas | Pedro Fontova / Las Puertas | **True** | -33.362828 $\rightarrow$ -33.362106 |
| **PB1343** | Calle G / General Gambino | Calle G / Juan Muñoz | **False** | -33.374486 $\rightarrow$ -33.374486 |
| **PD407** | (M) Príncipe de Gales | (M) Príncipe de Gales | **True** | -33.437824 $\rightarrow$ -33.437914 |
| **PD982** | Consistorial / Mar Pérsico | Consistorial / Mar Pérsico | **True** | -33.486431 $\rightarrow$ -33.486514 |
| **PE422** | Walker Martínez / Colombia | Walker Martínez / Colombia | **True** | -33.522580 $\rightarrow$ -33.522587 |
| **PF196** | 27 de Septiembre / El Mirador | 27 de Septiembre / El Mirador | **True** | -33.608050 $\rightarrow$ -33.607966 |
| **PF259** | Ángel Pimentel / Psje. Angel P. | Ángel Pimentel / Psje. Angel P. | **True** | -33.627033 $\rightarrow$ -33.627372 |
| **PF291** | Avenida Los Toros / Navarra | Avenida Los Toros / Navarra | **True** | -33.566689 $\rightarrow$ -33.566662 |
| **PF482** | Avenida Los Toros / San Sebastián | Avenida Los Toros / Navarra | **True** | -33.566567 $\rightarrow$ -33.566453 |
| **PH1502** | Salesianos / Romeo | Pdte. S. Allende G. / Romeo | **True** | -33.489668 $\rightarrow$ -33.489602 |

---

## 4. Oferta Global Diaria de Buses (`g_oferta_hora`)

La comparativa macro del sistema muestra estabilidad general en la provisión de servicios:

| Periodo Feed | Total de Salidas Diarias en la Red |
| :---: | :---: |
| **20260829 (V167)** | 14,464 |
| **20261003 (V168)** | 14,486 |
| **Variación Neta** | **+22 salidas / día** |