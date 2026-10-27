# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Verificación de resultados
# MAGIC
# MAGIC Este notebook no forma parte del Job: se ejecuta manualmente una vez que el Job finalizó correctamente con el mes completo (`hasta_fecha = 2026-12-31`). Reúne las consultas y visualizaciones que permiten verificar el resultado final:
# MAGIC
# MAGIC 1. Ingesta incremental en Bronze.
# MAGIC 2. Calidad de datos por fuente y detalle de la cuarentena.
# MAGIC 3. Comparación entre los casos inyectados por el generador (ground truth) y los detectados por el pipeline.
# MAGIC 4. Integridad de los archivos de liquidación.
# MAGIC 5. Indicadores por PSP.
# MAGIC 6. Monto no conciliado por capa y motivo.
# MAGIC 7. Evolución semanal de lo no conciliado.
# MAGIC 8. Paridad con el Data Warehouse original.
# MAGIC 9. Auditoría de las ejecuciones del Job.
# MAGIC
# MAGIC Las agregaciones se calculan con Spark; solo los resultados agregados se convierten a pandas para graficarlos.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "tp_final_conciliacion")
dbutils.widgets.text("ruta_base", "")

# COMMAND ----------

import matplotlib.pyplot as plt

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
base = dbutils.widgets.get("ruta_base").strip() or f"/Volumes/{catalogo}/{schema}"

spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {schema}")

for t in ["gold_calidad_datos", "gold_conc_operativa", "gold_kpi_psp", "gold_no_conciliadas", "pipeline_run_audit"]:
    assert spark.catalog.tableExists(t), f"Falta {t}. Ejecutar primero el Job completo."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingesta incremental en Bronze
# MAGIC Archivos y filas por fuente y por momento de ingesta. Con dos ejecuciones del Job (hasta el 15/01 y luego el mes completo) aparecen dos momentos distintos: la segunda ejecución cargó únicamente los archivos nuevos.

# COMMAND ----------

display(spark.sql("""
SELECT fuente, date_trunc('MINUTE', _fecha_ingesta) AS ingesta,
       COUNT(DISTINCT _archivo_origen) AS archivos, COUNT(*) AS filas
FROM (
  SELECT 'ventas' AS fuente, _archivo_origen, _fecha_ingesta FROM bronze_ventas
  UNION ALL SELECT 'eventos_mp',           _archivo_origen, _fecha_ingesta FROM bronze_eventos_mp
  UNION ALL SELECT 'eventos_prisma',       _archivo_origen, _fecha_ingesta FROM bronze_eventos_prisma
  UNION ALL SELECT 'liquidaciones_mp',     _archivo_origen, _fecha_ingesta FROM bronze_liquidaciones_mp
  UNION ALL SELECT 'liquidaciones_prisma', _archivo_origen, _fecha_ingesta FROM bronze_liquidaciones_prisma
  UNION ALL SELECT 'banco',                _archivo_origen, _fecha_ingesta FROM bronze_banco
)
GROUP BY ALL
ORDER BY fuente, ingesta
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calidad de datos por fuente

# COMMAND ----------

display(spark.table("gold_calidad_datos").orderBy("fuente"))

display(spark.sql("""
SELECT 'ventas' AS fuente, motivo_calidad, COUNT(*) AS filas,
       slice(collect_list(coalesce(total_amount_crudo, status_crudo, '(nulo)')), 1, 3) AS ejemplos
FROM silver_cuarentena_ventas GROUP BY ALL
UNION ALL
SELECT CASE id_psp WHEN 1 THEN 'eventos_mp' ELSE 'eventos_prisma' END, motivo_calidad, COUNT(*),
       slice(collect_list(coalesce(fecha_cruda, monto_bruto_crudo, '(nulo)')), 1, 3)
FROM silver_cuarentena_eventos_psp GROUP BY ALL
ORDER BY fuente, motivo_calidad
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Valores esperados y observados
# MAGIC El generador registró en `_control/ground_truth.jsonl` la cantidad de casos inyectados de cada tipo. Con el mes completo cargado, todas las filas deben presentar el estado `OK`.

# COMMAND ----------

spark.read.json(f"{base}/landing/fuente/_control/ground_truth.jsonl").createOrReplaceTempView("_ground_truth")

comparacion = spark.sql("""
WITH gt AS (
  SELECT tipo, ambito, metrica, CAST(esperado AS BIGINT) AS esperado
  FROM _ground_truth
  WHERE metrica NOT LIKE 'invalidas_%'
),
obs AS (
  SELECT 'calidad' AS tipo, fuente AS ambito, metrica, CAST(valor AS BIGINT) AS observado
  FROM gold_calidad_datos
  UNPIVOT (valor FOR metrica IN (filas_bronze AS filas_crudas, filas_estructurales AS registros_trailer,
                                 filas_cuarentena AS cuarentena, duplicados_removidos AS duplicados,
                                 filas_silver))
  UNION ALL
  SELECT 'calidad', 'ventas', 'estado_normalizado', COUNT_IF(estado_normalizado) FROM silver_trx_cliente
  UNION ALL
  SELECT 'conciliacion', 'OPERATIVA', estado_conciliacion, COUNT(*) FROM gold_conc_operativa GROUP BY ALL
  UNION ALL
  SELECT 'conciliacion', 'FINANCIERA', estado_conciliacion, COUNT(*) FROM gold_conc_financiera GROUP BY ALL
  UNION ALL
  SELECT 'conciliacion', 'BANCARIA', estado_conciliacion, COUNT(*) FROM gold_conc_bancaria GROUP BY ALL
)
SELECT
  coalesce(gt.tipo, obs.tipo)       AS tipo,
  coalesce(gt.ambito, obs.ambito)   AS ambito,
  coalesce(gt.metrica, obs.metrica) AS metrica,
  coalesce(gt.esperado, 0)          AS esperado,
  coalesce(obs.observado, 0)        AS observado,
  CASE WHEN coalesce(gt.esperado, 0) = coalesce(obs.observado, 0) THEN 'OK' ELSE 'DIFERENCIA' END AS control
FROM gt
FULL OUTER JOIN obs ON gt.tipo = obs.tipo AND gt.ambito = obs.ambito AND gt.metrica = obs.metrica
WHERE gt.metrica IS NOT NULL OR obs.tipo = 'conciliacion'
ORDER BY tipo, ambito, metrica
""")
display(comparacion)

resumen = comparacion.groupBy("control").count().collect()
print("Resultado de la comparación:", {r["control"]: r["count"] for r in resumen})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Integridad de los archivos de liquidación

# COMMAND ----------

display(spark.sql("""
SELECT CASE id_psp WHEN 1 THEN 'Mercado Pago' ELSE 'Prisma' END AS psp, estado,
       COUNT(*) AS archivos, SUM(diferencia) AS diferencia_total
FROM gold_control_archivos_liq
GROUP BY ALL ORDER BY psp, estado
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Indicadores por PSP

# COMMAND ----------

display(spark.sql("""
SELECT psp, capturas, monto_capturado, costo_efectivo_pct, dias_liquidacion_prom, plazo_contractual_dias,
       contracargos, tasa_contracargo_pct, tasa_conciliacion_operativa_pct,
       partidas_no_conciliadas, monto_no_conciliado,
       monto_nc_operativa, monto_nc_financiera, monto_nc_bancaria
FROM gold_kpi_psp ORDER BY psp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monto no conciliado por capa y motivo
# MAGIC **Pregunta:** ¿en qué capa y por qué motivo se concentra el monto no conciliado? Se emplea un gráfico de barras horizontales ordenado, que permite comparar magnitudes entre motivos; el color identifica la capa.

# COMMAND ----------

por_motivo = spark.sql("""
SELECT capa, motivo, COUNT(*) AS partidas, SUM(monto) AS monto
FROM gold_no_conciliadas
GROUP BY ALL
ORDER BY monto DESC
""")
display(por_motivo)

datos = por_motivo.toPandas().sort_values("monto")
datos["monto_m"] = datos["monto"].astype(float) / 1e6
colores = {"OPERATIVA": "#2563eb", "FINANCIERA": "#d97706", "BANCARIA": "#475569"}
fig, ax = plt.subplots(figsize=(9, 4.5))
ax.barh(datos["motivo"], datos["monto_m"], color=[colores[c] for c in datos["capa"]])
for y, (m, n) in enumerate(zip(datos["monto_m"], datos["partidas"])):
    ax.text(m + 0.2, y, f"${m:,.2f} M ({n} partidas)", va="center", fontsize=9)
ax.set_title("Monto no conciliado por motivo (enero–febrero 2026)")
ax.set_xlabel("Monto (millones de ARS)")
ax.set_xlim(0, datos["monto_m"].max() * 1.35)
ax.grid(axis="x", alpha=0.25)
ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=v) for v in colores.values()],
          labels=[k.capitalize() for k in colores], title="Capa", loc="lower right")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Evolución semanal de lo no conciliado (enero 2026)
# MAGIC **Pregunta:** ¿el monto no conciliado se mantiene estable a lo largo del mes? Se emplean barras apiladas por semana, que muestran a la vez el total semanal y la composición por motivo.

# COMMAND ----------

semanal = spark.sql("""
SELECT semana_del_mes, motivo, SUM(partidas) AS partidas, SUM(monto) AS monto
FROM gold_no_conciliadas_semana
WHERE mes = '2026-01'
GROUP BY ALL
ORDER BY semana_del_mes, motivo
""")
display(semanal)

pivote = (semanal.toPandas()
          .pivot(index="semana_del_mes", columns="motivo", values="monto")
          .fillna(0).astype(float) / 1e6)
pivote = pivote[pivote.sum().sort_values(ascending=False).index]
fig, ax = plt.subplots(figsize=(9, 4.5))
acumulado = None
for motivo in pivote.columns:
    ax.bar(pivote.index, pivote[motivo], bottom=acumulado, label=motivo)
    acumulado = pivote[motivo] if acumulado is None else acumulado + pivote[motivo]
ax.set_title("Monto no conciliado por semana y motivo (enero 2026)")
ax.set_xlabel("Semana del mes")
ax.set_ylabel("Monto (millones de ARS)")
ax.set_xticks(pivote.index)
ax.grid(axis="y", alpha=0.25)
ax.legend(fontsize=8, loc="upper left")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Paridad con el Data Warehouse original
# MAGIC Mismo corte que la consulta D.2 del trabajo de Almacenes de Datos (enero 2026). Los montos deben coincidir con los publicados en `resultados/analisis-consultas.md` del repositorio `conciliacion-pagos-digitales`.

# COMMAND ----------

display(spark.sql("""
SELECT n.motivo, COUNT(*) AS partidas, SUM(n.monto) AS monto,
       round(100 * SUM(n.monto) / (SELECT SUM(monto_orden) FROM silver_trx_cliente WHERE id_estado = 1), 3)
         AS pct_facturacion
FROM gold_no_conciliadas n
JOIN silver_dim_fecha f ON f.fecha = n.fecha
WHERE f.mes = '2026-01' AND n.motivo <> 'MOVIMIENTO_SIN_LOTE'
GROUP BY ALL
ORDER BY monto DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Auditoría de las ejecuciones del Job
# MAGIC Una fila por ejecución. La columna `idempotencia_comparada` indica si la ejecución repitió la fecha de corte de la anterior y, por lo tanto, si sus métricas se compararon y resultaron idénticas.

# COMMAND ----------

display(spark.table("pipeline_run_audit").orderBy("registrado_en"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Muestra de partidas para revisión

# COMMAND ----------

display(spark.sql("""
SELECT capa, motivo, fecha, psp, medio_pago, monto, comentario
FROM gold_no_conciliadas
WHERE motivo IN ('DIFERENCIA_MONTO', 'FANTASMA_PSP', 'SIN_ACREDITAR', 'MOVIMIENTO_SIN_LOTE')
QUALIFY row_number() OVER (PARTITION BY motivo ORDER BY monto DESC) <= 3
ORDER BY motivo, monto DESC
"""))
