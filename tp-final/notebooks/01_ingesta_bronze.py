# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingesta en Bronze
# MAGIC
# MAGIC Carga incremental de los archivos publicados en el Volume `raw`, una tabla Delta por fuente, mediante `COPY INTO`.
# MAGIC
# MAGIC - `COPY INTO` registra qué archivos ya cargó y, en cada ejecución, procesa únicamente los nuevos. Reejecutar la tarea no duplica filas.
# MAGIC - Todas las columnas se almacenan como texto (`inferSchema = false` en CSV y `primitivesAsString = true` en JSON). El tipado se realiza en Silver, donde cada descarte puede medirse.
# MAGIC - `mergeSchema = true` incorpora las columnas nuevas que aparecen a mitad de período (`coupon_code` en ventas e `installments` en Mercado Pago).
# MAGIC - Se agregan tres columnas de linaje: archivo de origen, fecha de modificación del archivo y momento de ingesta.
# MAGIC
# MAGIC Los datos maestros no pasan por Bronze: son tablas de referencia pequeñas que Silver lee completas en cada ejecución.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "tp_final_conciliacion")
dbutils.widgets.text("ruta_base", "")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
base = dbutils.widgets.get("ruta_base").strip() or f"/Volumes/{catalogo}/{schema}"
raw = f"{base}/raw"

spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {schema}")

# tabla destino, carpeta en raw, formato, opciones de lectura
FUENTES = [
    ("bronze_ventas", "ventas", "JSON", {"primitivesAsString": "true"}),
    ("bronze_eventos_mp", "eventos_mp", "CSV", {"header": "true", "sep": ",", "inferSchema": "false"}),
    ("bronze_eventos_prisma", "eventos_prisma", "CSV", {"header": "true", "sep": ";", "inferSchema": "false"}),
    ("bronze_liquidaciones_mp", "liquidaciones/mp", "CSV", {"header": "true", "sep": ",", "inferSchema": "false"}),
    ("bronze_liquidaciones_prisma", "liquidaciones/prisma", "CSV", {"header": "true", "sep": ";", "inferSchema": "false"}),
    ("bronze_banco", "banco", "CSV", {"header": "true", "sep": ";", "inferSchema": "false"}),
]

# COMMAND ----------

def copy_into(tabla, carpeta, formato, opciones):
    """Crea la tabla vacía si no existe y carga los archivos nuevos de la carpeta."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tabla}")
    opts = ", ".join(f"'{k}' = '{v}'" for k, v in opciones.items())
    spark.sql(f"""
COPY INTO {tabla}
FROM (
  SELECT *,
         _metadata.file_path              AS _archivo_origen,
         _metadata.file_modification_time AS _archivo_modificado,
         current_timestamp()              AS _fecha_ingesta
  FROM '{raw}/{carpeta}/'
)
FILEFORMAT = {formato}
FORMAT_OPTIONS ({opts})
COPY_OPTIONS ('mergeSchema' = 'true')
""")

# COMMAND ----------

resultados = []
for tabla, carpeta, formato, opciones in FUENTES:
    antes = spark.table(tabla).count() if spark.catalog.tableExists(tabla) else 0
    copy_into(tabla, carpeta, formato, opciones)
    t = spark.table(tabla)
    resultados.append((tabla, antes, t.count() - antes, t.count(),
                       t.select("_archivo_origen").distinct().count()))

display(spark.createDataFrame(
    resultados, "tabla string, filas_previas long, filas_nuevas long, filas_totales long, archivos long"))
