# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Validación del pipeline
# MAGIC
# MAGIC Última tarea del Job. Convierte los supuestos del diseño en controles ejecutables: si alguno falla, la tarea termina con error y el Job queda en estado fallido.
# MAGIC
# MAGIC Además, registra en `pipeline_run_audit` las métricas de cada ejecución. Cuando una ejecución repite la misma `hasta_fecha` que la anterior, verifica que las métricas no hayan cambiado, lo que demuestra que el pipeline es idempotente.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "tp_final_conciliacion")
dbutils.widgets.text("hasta_fecha", "2026-01-15")
dbutils.widgets.text("job_run_id", "interactive")

# COMMAND ----------

from datetime import date
from pyspark.sql import functions as F

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
hasta_fecha = date.fromisoformat(dbutils.widgets.get("hasta_fecha").strip())
job_run_id = dbutils.widgets.get("job_run_id").strip()[:100] or "interactive"

spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {schema}")

def valor(sql):
    return spark.sql(sql).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Controles
# MAGIC
# MAGIC | Control | Qué verifica |
# MAGIC |---|---|
# MAGIC | Claves únicas | Ninguna tabla de hechos de Silver contiene claves duplicadas |
# MAGIC | Balance por fuente | Las filas de Bronze se explican por Silver, cuarentena, registros estructurales y duplicados, sin valores negativos |
# MAGIC | Cobertura de la conciliación | Toda venta, todo evento liquidable y todo lote de Silver aparece en su capa de conciliación |
# MAGIC | Clasificación completa | Ninguna partida queda en el estado residual `REVISAR` |
# MAGIC | Integridad de archivos | Todos los archivos de liquidación coinciden con su registro trailer |
# MAGIC | Fecha de corte | No hay ventas posteriores a `hasta_fecha` |

# COMMAND ----------

checks = {
    "trx_sin_claves_duplicadas":
        valor("SELECT COUNT(*) = COUNT(DISTINCT id_trx) FROM silver_trx_cliente"),
    "eventos_sin_claves_duplicadas":
        valor("SELECT COUNT(*) = COUNT(DISTINCT id_psp, id_evento_psp) FROM silver_evento_psp"),
    "liquidaciones_sin_claves_duplicadas":
        valor("SELECT COUNT(*) = COUNT(DISTINCT id_psp, id_liquidacion) FROM silver_liquidacion_psp"),
    "banco_sin_claves_duplicadas":
        valor("SELECT COUNT(*) = COUNT(DISTINCT cuenta_bancaria, id_mov_banco) FROM silver_mov_bancario"),
    "balance_por_fuente_sin_negativos":
        valor("SELECT COUNT_IF(duplicados_removidos < 0) = 0 FROM gold_calidad_datos"),
    "toda_venta_en_conc_operativa":
        valor("""SELECT (SELECT COUNT(*) FROM silver_trx_cliente)
                      = (SELECT COUNT(DISTINCT id_trx) FROM gold_conc_operativa WHERE id_trx IS NOT NULL)"""),
    "todo_evento_liquidable_en_conc_financiera":
        valor("""SELECT (SELECT COUNT(*) FROM silver_evento_psp WHERE cod_evento IN ('CAPTURE', 'REFUND', 'CHBK'))
                      = (SELECT COUNT(*) FROM gold_conc_financiera WHERE id_evento_psp IS NOT NULL)"""),
    "todo_lote_en_conc_bancaria":
        valor("""SELECT (SELECT COUNT(DISTINCT id_lote_liquidacion) FROM silver_liquidacion_psp)
                      = (SELECT COUNT(DISTINCT id_lote_liquidacion) FROM gold_conc_bancaria
                         WHERE id_lote_liquidacion IS NOT NULL)"""),
    "sin_partidas_sin_clasificar":
        valor("SELECT COUNT(*) = 0 FROM gold_conc_operativa WHERE estado_conciliacion = 'REVISAR'"),
    "archivos_liquidacion_cuadrados":
        valor("SELECT COUNT(*) > 0 AND COUNT_IF(estado <> 'OK') = 0 FROM gold_control_archivos_liq"),
    "ventas_dentro_de_la_fecha_de_corte":
        valor(f"SELECT coalesce(MAX(fecha_trx_cliente) <= DATE'{hasta_fecha}', false) FROM silver_trx_cliente"),
}

advertencias = {
    "ordenes_con_mas_de_un_sku": valor("SELECT COUNT_IF(lineas_en_orden <> 1) FROM silver_trx_cliente"),
}

display(spark.createDataFrame([(k, bool(v)) for k, v in checks.items()], "control string, resultado boolean"))
print("Advertencias (no detienen el Job):", advertencias)

fallidos = [k for k, v in checks.items() if not v]
assert not fallidos, f"Controles fallidos: {fallidos}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auditoría e idempotencia

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_run_audit (
  job_run_id STRING, hasta_fecha DATE, filas_bronze BIGINT, filas_silver BIGINT, filas_cuarentena BIGINT,
  partidas_no_conciliadas BIGINT, monto_no_conciliado DECIMAL(24,2), registrado_en TIMESTAMP,
  idempotencia_comparada BOOLEAN
) USING DELTA
""")

calidad = spark.table("gold_calidad_datos").agg(
    F.sum("filas_bronze").alias("bronze"), F.sum("filas_silver").alias("silver"),
    F.sum("filas_cuarentena").alias("cuarentena")).first()
nc = spark.table("gold_no_conciliadas").agg(F.count("*").alias("n"), F.sum("monto").alias("monto")).first()
actual = (int(calidad.bronze), int(calidad.silver), int(calidad.cuarentena), int(nc.n), nc.monto)

previa = (spark.table("pipeline_run_audit").orderBy(F.col("registrado_en").desc())
          .select("hasta_fecha", "filas_bronze", "filas_silver", "filas_cuarentena",
                  "partidas_no_conciliadas", "monto_no_conciliado").first())
comparable = previa is not None and previa.hasta_fecha == hasta_fecha
if comparable:
    anterior = (previa.filas_bronze, previa.filas_silver, previa.filas_cuarentena,
                previa.partidas_no_conciliadas, previa.monto_no_conciliado)
    assert actual == anterior, f"La reejecución modificó las métricas: antes={anterior}, ahora={actual}"

fila = spark.createDataFrame(
    [(job_run_id, hasta_fecha, *actual)],
    "job_run_id string, hasta_fecha date, filas_bronze long, filas_silver long, filas_cuarentena long, "
    "partidas_no_conciliadas long, monto_no_conciliado decimal(24,2)"
).select("*", F.current_timestamp().alias("registrado_en"), F.lit(comparable).alias("idempotencia_comparada"))
fila.write.mode("append").saveAsTable("pipeline_run_audit")

display(spark.table("pipeline_run_audit").orderBy("registrado_en"))
