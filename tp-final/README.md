# Trabajo Práctico Final — Big Data y MLOps

- **Estudiante:** Santino Cantisano
- **Variante:** 2 — Ejecución
- **Opción:** A — Arquitectura medallion en Databricks

## Objetivo

El objetivo del trabajo es diseñar e implementar en Databricks una arquitectura medallion (Bronze, Silver y Gold) orientada a la conciliación de pagos digitales de una empresa que opera con dos proveedores de servicios de pago (PSP): Mercado Pago y Prisma. El pipeline ingiere los archivos crudos provenientes de la plataforma de ventas, de ambos PSP y de la entidad bancaria; los depura, los integra y efectúa el cruce de las tres capas de la conciliación: venta contra PSP, PSP contra liquidación y liquidación contra banco.

Cada fuente presenta un formato propio (JSON, CSV delimitado por coma o por punto y coma, montos con coma decimal, fechas en UTC o en formato dd/mm/yyyy) e incorpora errores de calidad representativos de integraciones reales, tales como reenvíos de registros, montos no numéricos o la aparición de columnas nuevas a mitad de período. La capa Silver tiene a su cargo el tratamiento de dichos errores, mientras que las discrepancias de conciliación (ventas sin cobro asociado, lotes sin acreditar, entre otras) se preservan íntegramente hasta la capa Gold, donde son detectadas y clasificadas.

Los pasos de transformación se implementan como notebooks y se orquestan mediante un Job de Databricks, compuesto por cinco tareas: publicación de archivos, ingesta en Bronze, construcción de Silver, construcción de Gold y validación.

## Relación con un trabajo anterior

El modelo de datos y la lógica de conciliación provienen del trabajo final de la materia Almacenes de Datos y Procesamiento Analítico en Línea ([conciliacion-pagos-digitales](https://github.com/SantinoCantisano/conciliacion-pagos-digitales)), en el cual se desarrolló un Data Warehouse en PostgreSQL a partir de datos cargados directamente en las tablas finales.

El presente trabajo incorpora los siguientes aportes:
- la generación de archivos crudos heterogéneos con errores de calidad controlados;
- la ingesta incremental e idempotente mediante `COPY INTO`;
- la capa Silver, que comprende tipado, normalización entre PSP, reglas de calidad con cuarentena, deduplicación e integración incremental mediante `MERGE`;
- la traducción de la lógica de conciliación de PostgreSQL a Spark SQL;
- la orquestación de las tareas mediante un Job de Databricks;
- la validación automática del pipeline en cada ejecución y la verificación de los resultados contra los casos inyectados en los datos.

Los montos obtenidos en la capa Gold coinciden exactamente con los del Data Warehouse original ([detalle](docs/04-resultados.md#paridad-con-el-dw-original)).

## Contenido de la carpeta

```
tp-final/
├── README.md
├── docs/
│   ├── 01-datos.md                ← origen, esquema de entrada, criterio de elección y errores inyectados
│   ├── 02-arquitectura.md         ← diagrama, tecnologías, configuración y orden del Job, decisiones técnicas
│   ├── 03-modelo.md               ← tablas por capa, diagrama del modelo, reglas de transformación y calidad
│   └── 04-resultados.md           ← resultados, validación, comparación con el DW original, escala y limitaciones
├── generador/
│   └── generar_datos.py           ← generación de los archivos crudos y del ground truth
├── datos/raw/                     ← archivos generados (253 archivos, 2,6 MB) y _control/ground_truth.jsonl
├── notebooks/
│   ├── 00_publicar_archivos.py    ← tarea 1: schema, Volumes y publicación de archivos por fecha de corte
│   ├── 01_ingesta_bronze.py       ← tarea 2: ingesta incremental con COPY INTO
│   ├── 02_build_silver.py         ← tarea 3: tipado, calidad, cuarentena, deduplicación y MERGE
│   ├── 03_build_gold.py           ← tarea 4: conciliación en tres capas y productos de datos
│   ├── 04_validate_pipeline.py    ← tarea 5: controles ejecutables y auditoría de idempotencia
│   └── 05_verificacion.py         ← consultas y visualizaciones de verificación (ejecución manual)
├── job/
│   └── job_config.json            ← definición del Job (tareas, dependencias y parámetros)
├── tests/
│   └── ejecutar_job_local.py      ← ejecución local de los notebooks con PySpark y contraste con el ground truth
└── evidencia/                     ← capturas de la ejecución en Databricks
```

## Reproducción en Databricks

La implementación está diseñada para Databricks Free Edition (catálogo `workspace`, cómputo serverless). En otro workspace, deben modificarse los parámetros `catalogo` y `schema` del Job.

1. Clonar el repositorio como *Git folder* (*Workspace → Create → Git folder*) o, si ya se encuentra clonado, actualizarlo mediante *Pull*.
2. Crear el Job desde *Jobs & Pipelines → Create → Job*, con cinco tareas de tipo *Notebook*, origen *Workspace* y cómputo serverless, encadenadas en el siguiente orden:

   | Tarea | Notebook | Depende de |
   |---|---|---|
   | `publicar_archivos` | `tp-final/notebooks/00_publicar_archivos` | — |
   | `ingesta_bronze` | `tp-final/notebooks/01_ingesta_bronze` | `publicar_archivos` |
   | `build_silver` | `tp-final/notebooks/02_build_silver` | `ingesta_bronze` |
   | `build_gold` | `tp-final/notebooks/03_build_gold` | `build_silver` |
   | `validate_pipeline` | `tp-final/notebooks/04_validate_pipeline` | `build_gold` |

   En la configuración del Job deben definirse los parámetros `catalogo = workspace`, `schema = tp_final_conciliacion` y `hasta_fecha = 2026-01-15`. En la tarea `validate_pipeline` debe agregarse el parámetro `job_run_id = {{job.run_id}}`. El archivo `job/job_config.json` contiene la misma definición en formato JSON; las rutas deben ajustarse al usuario del workspace.
3. Ejecutar el Job (*Run now*). La primera ejecución procesa la primera quincena del mes.
4. Ejecutar nuevamente el Job mediante *Run now with different parameters*, con `hasta_fecha = 2026-12-31`. En esta segunda ejecución, la capa Bronze procesa únicamente los archivos nuevos.
5. Ejecutar el Job una tercera vez con `hasta_fecha = 2026-12-31`. La tarea de validación compara las métricas con las de la ejecución anterior y verifica que no se modificaron.
6. Ejecutar manualmente `tp-final/notebooks/05_verificacion`. En la celda de comparación entre valores esperados y observados, todas las filas deben presentar el estado `OK`.

Para reiniciar el proceso desde cero, debe ejecutarse la última celda de `00_publicar_archivos`, que elimina el schema junto con sus tablas y Volumes.

## Reproducción local

```bash
# 1. Regenerar los archivos (opcional: se encuentran versionados en datos/raw y la semilla fija garantiza el mismo resultado)
python generador/generar_datos.py --salida datos/raw

# 2. Ejecutar los notebooks del Job con PySpark (tres ejecuciones y verificación)
pip install pyspark matplotlib    # requiere Java 17 o superior
python tests/ejecutar_job_local.py
```

El script ejecuta el código de los notebooks sin modificaciones y emula únicamente las construcciones propias de Databricks que no existen en Spark de código abierto (`COPY INTO`, `MERGE`, tablas Delta, Volumes y `dbutils.widgets`). Su propósito es validar la lógica de transformación; la evidencia formal de la entrega corresponde a la ejecución en Databricks.

## Fuentes de datos y referencias

- **Datos:** sintéticos, generados mediante `generador/generar_datos.py`. No contienen datos personales reales: los usuarios se representan con identificadores numéricos y con email y documento seudonimizados mediante SHA-256 a partir de cadenas ficticias. El repositorio no contiene credenciales.
- **Modelo y lógica de conciliación:** [conciliacion-pagos-digitales](https://github.com/SantinoCantisano/conciliacion-pagos-digitales), trabajo propio de la materia Almacenes de Datos y Procesamiento Analítico en Línea.
- **Documentación de Databricks:**
  - `COPY INTO` — <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/>
  - `MERGE INTO` — <https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into>
  - Columna de metadatos de archivos (`_metadata`) — <https://docs.databricks.com/aws/ingestion/file-metadata-column>
  - Lakeflow Jobs — <https://docs.databricks.com/aws/en/jobs/>
  - Referencias dinámicas en parámetros de Jobs (`{{job.run_id}}`) — <https://docs.databricks.com/aws/jobs/dynamic-value-references>
  - Unity Catalog Volumes — <https://docs.databricks.com/aws/en/volumes/>
  - Límites de Free Edition — <https://docs.databricks.com/aws/en/getting-started/free-edition-limitations>
