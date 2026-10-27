# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Construcción de Gold
# MAGIC
# MAGIC Gold concilia las tres capas del ciclo de un pago con la lógica del Data Warehouse original, traducida de PostgreSQL a Spark SQL, y publica los productos de datos:
# MAGIC
# MAGIC | Tabla | Grano |
# MAGIC |---|---|
# MAGIC | `gold_conc_operativa` | Una fila por (venta, operación PSP) |
# MAGIC | `gold_conc_financiera` | Una fila por (evento, liquidación) |
# MAGIC | `gold_conc_bancaria` | Una fila por (lote, movimiento bancario) |
# MAGIC | `gold_no_conciliadas` | Una fila por partida no conciliada |
# MAGIC | `gold_kpi_psp` | Una fila por PSP |
# MAGIC | `gold_no_conciliadas_semana` | Mes × semana × PSP × capa × motivo |
# MAGIC | `gold_control_archivos_liq` | Una fila por archivo de liquidación |
# MAGIC | `gold_calidad_datos` | Una fila por fuente |
# MAGIC
# MAGIC Todas las tablas se reconstruyen completas desde Silver en cada ejecución, de modo que el resultado es determinista y una reejecución produce exactamente los mismos valores.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "tp_final_conciliacion")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {schema}")

for t in ["silver_trx_cliente", "silver_evento_psp", "silver_liquidacion_psp", "silver_liquidacion_trailer",
          "silver_mov_bancario", "silver_mov_bancario_lote", "silver_dim_psp", "silver_dim_medio_pago",
          "silver_dim_estado_pago", "silver_dim_fecha"]:
    assert spark.catalog.tableExists(t), f"Falta {t}. Ejecutar la tarea 02."

# COMMAND ----------

# MAGIC %md
# MAGIC El PSP desconoce el identificador de la venta: el vínculo entre ambos se resuelve por la referencia de pago que la plataforma envía al PSP. Un evento cuya referencia no existe en la plataforma queda con `id_trx` nulo.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _eventos_con_trx AS
SELECT e.*, t.id_trx
FROM silver_evento_psp e
LEFT JOIN silver_trx_cliente t ON t.referencia_pago = e.external_reference
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Conciliación operativa: venta contra eventos del PSP
# MAGIC `FULL OUTER JOIN` entre cada venta y los eventos agrupados por operación. Un evento sin venta asociada es un fantasma PSP; una venta aprobada sin eventos es un fantasma cliente.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_conc_operativa
COMMENT 'Venta vs. eventos del PSP. Una fila por (id_trx, operación PSP).'
AS
WITH psp_side AS (
  SELECT
    id_trx,
    id_operacion_psp,
    MIN(id_psp)                                                  AS id_psp,
    MIN(id_medio)                                                AS id_medio,
    MIN(fecha_evento) FILTER (WHERE cod_evento = 'CAPTURE')      AS fecha_capture,
    bool_or(cod_evento = 'AUTH')                                 AS tuvo_auth,
    bool_or(cod_evento = 'CAPTURE')                              AS tuvo_capture,
    bool_or(cod_evento = 'REFUND')                               AS tuvo_refund,
    bool_or(cod_evento = 'CHBK')                                 AS tuvo_chbk,
    SUM(monto_bruto) FILTER (WHERE cod_evento = 'CAPTURE')       AS monto_bruto_psp,
    SUM(fee_psp)     FILTER (WHERE cod_evento = 'CAPTURE')       AS fee_psp,
    SUM(impuestos)   FILTER (WHERE cod_evento = 'CAPTURE')       AS impuestos_psp,
    -- equivalente a STRING_AGG(cod_evento ORDER BY fecha_evento, cod_evento)
    array_join(transform(array_sort(collect_list(struct(fecha_evento, cod_evento))),
                         x -> x.cod_evento), ',')                AS eventos
  FROM _eventos_con_trx
  GROUP BY id_trx, id_operacion_psp
)
SELECT
  md5(concat_ws('|', coalesce(CAST(t.id_trx AS STRING), 'NULL'),
                     coalesce(CAST(e.id_operacion_psp AS STRING), 'NULL'), 'OP')) AS id_conciliacion,
  t.id_trx,
  t.fecha_trx_cliente,
  t.monto_orden,
  t.id_estado,
  est.estado_pago AS estado_trx,
  e.id_operacion_psp,
  e.eventos,
  e.fecha_capture,
  e.monto_bruto_psp,
  e.fee_psp,
  e.impuestos_psp,
  e.id_psp,
  e.id_medio,
  psp.nombre AS psp,
  mp.nombre  AS medio_pago,
  CASE
    WHEN t.id_trx IS NULL                                        THEN 'FANTASMA_PSP'
    WHEN e.id_operacion_psp IS NULL AND t.id_estado = 1          THEN 'FANTASMA_CLIENTE'
    WHEN e.id_operacion_psp IS NULL AND t.id_estado = 2          THEN 'RECHAZO_SIN_INTENTO_PSP'
    WHEN t.id_estado = 2 AND e.tuvo_auth AND NOT e.tuvo_capture  THEN 'RECHAZO_OK'
    WHEN t.id_estado = 2 AND e.tuvo_capture                      THEN 'RECHAZO_INCONSISTENTE'
    WHEN t.id_estado = 4 AND e.tuvo_refund                       THEN 'DEVOLUCION_OK'
    WHEN t.id_estado = 5 AND e.tuvo_chbk                         THEN 'CONTRACARGO_OK'
    -- la venta ya figura como devuelta o con contracargo, pero el evento del PSP (T+3 / T+5) aún no llegó
    WHEN t.id_estado = 4 AND e.tuvo_capture AND NOT e.tuvo_refund THEN 'DEVOLUCION_PENDIENTE'
    WHEN t.id_estado = 5 AND e.tuvo_capture AND NOT e.tuvo_chbk   THEN 'CONTRACARGO_PENDIENTE'
    WHEN t.id_estado = 1 AND NOT e.tuvo_capture                  THEN 'SIN_CAPTURA'
    WHEN t.id_estado = 1 AND e.tuvo_capture
         AND e.monto_bruto_psp <> t.monto_orden                  THEN 'DIFERENCIA_MONTO'
    WHEN t.id_estado = 1 AND e.tuvo_capture
         AND e.monto_bruto_psp = t.monto_orden                   THEN 'OK_CONCILIADO'
    ELSE 'REVISAR'
  END AS estado_conciliacion,
  CASE
    WHEN t.id_trx IS NULL
      THEN concat('Evento PSP sin trx cliente. id_op=', CAST(e.id_operacion_psp AS STRING),
                  ' monto=', CAST(e.monto_bruto_psp AS STRING))
    WHEN e.id_operacion_psp IS NULL AND t.id_estado = 1
      THEN 'Trx aprobada sin ningún evento en el PSP'
    WHEN t.id_estado = 1 AND e.tuvo_capture AND e.monto_bruto_psp <> t.monto_orden
      THEN concat('Monto: orden=', CAST(t.monto_orden AS STRING),
                  ' psp=', CAST(e.monto_bruto_psp AS STRING),
                  ' delta=', CAST(e.monto_bruto_psp - t.monto_orden AS STRING))
    WHEN t.id_estado = 1 AND NOT e.tuvo_capture
      THEN concat('Trx aprobada pero PSP solo registró: ', coalesce(e.eventos, '(nada)'))
    WHEN t.id_estado = 2 AND e.tuvo_capture
      THEN concat('Trx rechazada pero PSP registró CAPTURE (eventos: ', e.eventos, ')')
    WHEN t.id_estado IN (4, 5) AND e.tuvo_capture AND NOT (e.tuvo_refund OR e.tuvo_chbk)
      THEN concat('Venta ', est.estado_pago, ' sin el evento correspondiente en el PSP (eventos: ', e.eventos, ')')
  END AS comentario
FROM silver_trx_cliente t
FULL OUTER JOIN psp_side e        ON e.id_trx = t.id_trx
LEFT JOIN silver_dim_estado_pago est ON est.id_estado = t.id_estado
LEFT JOIN silver_dim_psp psp         ON psp.id_psp = e.id_psp
LEFT JOIN silver_dim_medio_pago mp   ON mp.id_medio = e.id_medio
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conciliación financiera: evento contra liquidación
# MAGIC Se excluyen las autorizaciones, que no se liquidan. El plazo contractual se obtiene del maestro de PSP.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_conc_financiera
COMMENT 'Evento PSP vs. liquidación. Detecta pendientes, huérfanas, diferencias y atrasos.'
AS
WITH ev AS (
  SELECT * FROM _eventos_con_trx WHERE cod_evento IN ('CAPTURE', 'REFUND', 'CHBK')
)
SELECT
  md5(concat_ws('|', coalesce(CAST(e.id_evento_psp AS STRING), 'NULL'),
                     coalesce(CAST(l.id_liquidacion AS STRING), 'NULL'), 'FIN')) AS id_conciliacion,
  e.id_evento_psp,
  e.fecha_evento,
  e.cod_evento     AS ev_cod,
  e.id_trx,
  e.id_medio,
  e.monto_bruto    AS monto_bruto_ev,
  e.monto_neto     AS monto_neto_ev,
  e.fee_psp        AS fee_ev,
  e.impuestos      AS impuestos_ev,
  l.id_liquidacion,
  l.fecha_liquidacion,
  l.cod_evento     AS liq_cod,
  l.monto_liquidado,
  l.fees_liquid,
  l.retenciones,
  l.contracargos,
  l.id_lote_liquidacion,
  coalesce(e.id_psp, l.id_psp)                     AS id_psp,
  psp.nombre                                       AS psp,
  mp.nombre                                        AS medio_pago,
  datediff(l.fecha_liquidacion, e.fecha_evento)    AS dias_a_liquidar,
  CASE
    WHEN e.id_evento_psp IS NULL                                     THEN 'LIQUIDACION_HUERFANA'
    WHEN l.id_liquidacion IS NULL                                    THEN 'PENDIENTE_LIQUIDACION'
    WHEN e.cod_evento <> l.cod_evento                                THEN 'DIFERENCIA_TIPO_EVENTO'
    WHEN e.fee_psp <> l.fees_liquid                                  THEN 'DIFERENCIA_FEE'
    WHEN datediff(l.fecha_liquidacion, e.fecha_evento) > psp.plazo_liquidacion_dias
                                                                     THEN 'FUERA_DE_PLAZO'
    ELSE 'OK_LIQUIDADO'
  END AS estado_conciliacion,
  CASE
    WHEN e.id_evento_psp IS NULL  THEN 'Liquidación sin evento PSP identificable'
    WHEN l.id_liquidacion IS NULL
      THEN concat('Evento ', e.cod_evento, ' del ', CAST(e.fecha_evento AS STRING), ' sin liquidación asociada')
    WHEN e.cod_evento <> l.cod_evento
      THEN concat('Tipo: ev=', e.cod_evento, ' liq=', l.cod_evento)
    WHEN e.fee_psp <> l.fees_liquid
      THEN concat('Fee: ev=', CAST(e.fee_psp AS STRING), ' liq=', CAST(l.fees_liquid AS STRING))
    WHEN datediff(l.fecha_liquidacion, e.fecha_evento) > psp.plazo_liquidacion_dias
      THEN concat('Liq tardía: ', CAST(datediff(l.fecha_liquidacion, e.fecha_evento) AS STRING),
                  ' días (esperado ', CAST(psp.plazo_liquidacion_dias AS STRING), ')')
  END AS comentario
FROM ev e
FULL OUTER JOIN silver_liquidacion_psp l
  ON l.id_evento_psp = e.id_evento_psp AND l.id_psp = e.id_psp
LEFT JOIN silver_dim_psp psp       ON psp.id_psp = coalesce(e.id_psp, l.id_psp)
LEFT JOIN silver_dim_medio_pago mp ON mp.id_medio = e.id_medio
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Conciliación bancaria: lote contra movimiento
# MAGIC El cruce se resuelve por igualdad a través de la tabla puente. Se incluyen también los movimientos bancarios cuyo concepto no menciona ningún lote existente.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_conc_bancaria
COMMENT 'Lote de liquidación vs. movimiento bancario. Incluye movimientos sin lote reconocible.'
AS
WITH lote_totales AS (
  SELECT id_psp, id_lote_liquidacion, fecha_liquidacion,
         SUM(monto_liquidado) AS monto_lote,
         COUNT(*)             AS liquis_en_lote
  FROM silver_liquidacion_psp
  GROUP BY id_psp, id_lote_liquidacion, fecha_liquidacion
),
lote_mov AS (
  SELECT l.*, m.id_mov_banco, m.fecha_movimiento, m.fecha_correspondiente,
         m.importe, m.descripcion, m.cuenta_bancaria
  FROM lote_totales l
  LEFT JOIN silver_mov_bancario_lote b ON b.id_lote_liquidacion = l.id_lote_liquidacion
  LEFT JOIN silver_mov_bancario m      ON m.id_mov_banco = b.id_mov_banco
                                      AND m.cuenta_bancaria = b.cuenta_bancaria
),
banco_huerfano AS (
  SELECT m.*
  FROM silver_mov_bancario m
  LEFT ANTI JOIN (
    SELECT b.id_mov_banco, b.cuenta_bancaria
    FROM silver_mov_bancario_lote b
    JOIN lote_totales l ON l.id_lote_liquidacion = b.id_lote_liquidacion
  ) x ON x.id_mov_banco = m.id_mov_banco AND x.cuenta_bancaria = m.cuenta_bancaria
),
unido AS (
  SELECT id_lote_liquidacion, fecha_liquidacion, monto_lote, liquis_en_lote, id_psp,
         id_mov_banco, fecha_movimiento, fecha_correspondiente, importe, descripcion, cuenta_bancaria
  FROM lote_mov
  UNION ALL
  SELECT CAST(NULL AS STRING), CAST(NULL AS DATE), CAST(NULL AS DECIMAL(24,2)), CAST(NULL AS BIGINT),
         CAST(NULL AS INT), id_mov_banco, fecha_movimiento, fecha_correspondiente, importe,
         descripcion, cuenta_bancaria
  FROM banco_huerfano
)
SELECT
  md5(concat_ws('|', coalesce(x.id_lote_liquidacion, 'NULL'),
                     coalesce(CAST(x.id_mov_banco AS STRING), 'NULL'), 'BANK')) AS id_conciliacion,
  x.id_lote_liquidacion,
  x.fecha_liquidacion,
  x.monto_lote,
  x.liquis_en_lote,
  coalesce(x.id_psp, p2.id_psp)                       AS id_psp,
  coalesce(p1.nombre, p2.nombre)                      AS psp,
  x.id_mov_banco,
  x.fecha_movimiento,
  x.fecha_correspondiente,
  x.importe                                           AS importe_banco,
  x.descripcion                                       AS desc_banco,
  x.cuenta_bancaria,
  datediff(x.fecha_movimiento, x.fecha_liquidacion)   AS dias_diferencia,
  CASE
    WHEN x.id_mov_banco IS NULL                       THEN 'SIN_ACREDITAR'
    WHEN x.id_lote_liquidacion IS NULL                THEN 'MOVIMIENTO_SIN_LOTE'
    WHEN x.fecha_movimiento <> x.fecha_liquidacion    THEN 'FECHA_ACREDITACION_DISTINTA'
    ELSE 'OK_ACREDITADO'
  END AS estado_conciliacion,
  CASE
    WHEN x.id_mov_banco IS NULL
      THEN concat('Lote ', x.id_lote_liquidacion, ' (', CAST(x.monto_lote AS STRING),
                  ') no aparece en ningún movimiento bancario')
    WHEN x.id_lote_liquidacion IS NULL
      THEN concat('Movimiento banco sin lote reconocible: ', x.descripcion)
    WHEN x.fecha_movimiento <> x.fecha_liquidacion
      THEN concat('Fecha: liqui=', CAST(x.fecha_liquidacion AS STRING),
                  ' banco=', CAST(x.fecha_movimiento AS STRING))
  END AS comentario
FROM unido x
LEFT JOIN silver_dim_psp p1 ON p1.id_psp = x.id_psp
LEFT JOIN silver_dim_psp p2 ON x.id_psp IS NULL AND p2.cuenta_bancaria = x.cuenta_bancaria
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partidas no conciliadas
# MAGIC Unificación de todo lo que no concilia en las tres capas, con capa, motivo, monto y comentario.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_no_conciliadas
COMMENT 'Partidas no conciliadas de las 3 capas, con motivo, monto y comentario.'
AS
SELECT id_conciliacion, 'OPERATIVA' AS capa, estado_conciliacion AS motivo,
       coalesce(fecha_trx_cliente, fecha_capture) AS fecha,
       id_psp, id_medio, psp, medio_pago,
       CAST(coalesce(monto_orden, monto_bruto_psp, 0) AS DECIMAL(24,2)) AS monto,
       comentario
FROM gold_conc_operativa
WHERE estado_conciliacion IN ('FANTASMA_CLIENTE', 'FANTASMA_PSP', 'DIFERENCIA_MONTO',
                              'SIN_CAPTURA', 'RECHAZO_INCONSISTENTE', 'DEVOLUCION_PENDIENTE',
                              'CONTRACARGO_PENDIENTE', 'REVISAR')
UNION ALL
SELECT id_conciliacion, 'FINANCIERA', estado_conciliacion,
       coalesce(fecha_evento, fecha_liquidacion),
       id_psp, id_medio, psp, medio_pago,
       CAST(coalesce(abs(monto_neto_ev), abs(monto_liquidado), 0) AS DECIMAL(24,2)),
       comentario
FROM gold_conc_financiera
WHERE estado_conciliacion <> 'OK_LIQUIDADO'
UNION ALL
SELECT id_conciliacion, 'BANCARIA', estado_conciliacion,
       coalesce(fecha_liquidacion, fecha_movimiento),
       id_psp, CAST(NULL AS INT), psp, CAST(NULL AS STRING),
       CAST(coalesce(abs(monto_lote), abs(importe_banco), 0) AS DECIMAL(24,2)),
       comentario
FROM gold_conc_bancaria
WHERE estado_conciliacion <> 'OK_ACREDITADO'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Producto de datos: indicadores por PSP

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_kpi_psp
COMMENT 'KPIs por PSP: volumen, costo efectivo, plazo de liquidación, contracargos y conciliación.'
AS
WITH capturas AS (
  SELECT id_psp,
         COUNT(*)                         AS capturas,
         SUM(monto_bruto)                 AS monto_capturado,
         SUM(fee_psp + impuestos)         AS costo_psp
  FROM silver_evento_psp
  WHERE cod_evento = 'CAPTURE'
  GROUP BY id_psp
),
chbk AS (
  SELECT id_psp, COUNT(*) AS contracargos, SUM(abs(monto_bruto)) AS monto_contracargos
  FROM silver_evento_psp WHERE cod_evento = 'CHBK' GROUP BY id_psp
),
liq AS (
  SELECT id_psp,
         SUM(monto_liquidado) AS monto_liquidado,
         AVG(dias_a_liquidar) AS dias_liquidacion_prom
  FROM gold_conc_financiera
  WHERE id_liquidacion IS NOT NULL AND id_evento_psp IS NOT NULL
  GROUP BY id_psp
),
conc AS (
  SELECT id_psp,
         COUNT(*)                                                    AS ventas_aprobadas,
         COUNT_IF(estado_conciliacion = 'OK_CONCILIADO')             AS ventas_conciliadas
  FROM gold_conc_operativa
  WHERE id_estado = 1 AND id_psp IS NOT NULL
  GROUP BY id_psp
),
nc AS (
  SELECT id_psp,
         COUNT(*)                                      AS partidas_no_conciliadas,
         SUM(monto)                                    AS monto_no_conciliado,
         SUM(monto) FILTER (WHERE capa = 'OPERATIVA')  AS monto_nc_operativa,
         SUM(monto) FILTER (WHERE capa = 'FINANCIERA') AS monto_nc_financiera,
         SUM(monto) FILTER (WHERE capa = 'BANCARIA')   AS monto_nc_bancaria
  FROM gold_no_conciliadas
  GROUP BY id_psp
)
SELECT
  p.id_psp,
  p.nombre                                                         AS psp,
  c.capturas,
  c.monto_capturado,
  c.costo_psp,
  round(100 * c.costo_psp / c.monto_capturado, 2)                  AS costo_efectivo_pct,
  l.monto_liquidado,
  round(l.dias_liquidacion_prom, 2)                                AS dias_liquidacion_prom,
  p.plazo_liquidacion_dias                                         AS plazo_contractual_dias,
  coalesce(k.contracargos, 0)                                      AS contracargos,
  coalesce(k.monto_contracargos, 0)                                AS monto_contracargos,
  round(100 * coalesce(k.contracargos, 0) / c.capturas, 2)         AS tasa_contracargo_pct,
  -- Ventas aprobadas atribuibles al PSP (las que no tienen ningún evento no tienen PSP)
  conc.ventas_aprobadas,
  round(100 * conc.ventas_conciliadas / conc.ventas_aprobadas, 2)  AS tasa_conciliacion_operativa_pct,
  coalesce(nc.partidas_no_conciliadas, 0)                          AS partidas_no_conciliadas,
  coalesce(nc.monto_no_conciliado, 0)                              AS monto_no_conciliado,
  coalesce(nc.monto_nc_operativa, 0)                               AS monto_nc_operativa,
  coalesce(nc.monto_nc_financiera, 0)                              AS monto_nc_financiera,
  coalesce(nc.monto_nc_bancaria, 0)                                AS monto_nc_bancaria
FROM silver_dim_psp p
LEFT JOIN capturas c ON c.id_psp = p.id_psp
LEFT JOIN chbk k     ON k.id_psp = p.id_psp
LEFT JOIN liq l      ON l.id_psp = p.id_psp
LEFT JOIN conc       ON conc.id_psp = p.id_psp
LEFT JOIN nc         ON nc.id_psp = p.id_psp
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Producto de datos: evolución semanal de lo no conciliado

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_no_conciliadas_semana
COMMENT 'Partidas y monto no conciliado por mes, semana del mes, PSP, capa y motivo.'
AS SELECT
  f.mes,
  f.semana_del_mes,
  coalesce(n.psp, '(sin PSP)') AS psp,
  n.capa,
  n.motivo,
  COUNT(*)     AS partidas,
  SUM(n.monto) AS monto
FROM gold_no_conciliadas n
JOIN silver_dim_fecha f ON f.fecha = n.fecha
GROUP BY f.mes, f.semana_del_mes, coalesce(n.psp, '(sin PSP)'), n.capa, n.motivo
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Control de integridad de los archivos de liquidación

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_control_archivos_liq
COMMENT 'Por archivo de liquidación: total declarado en el registro trailer vs. suma de los registros de detalle.'
AS
WITH detalle AS (
  SELECT id_psp, _archivo_origen, COUNT(*) AS registros_detalle, SUM(monto_liquidado) AS suma_detalle,
         MAX(fecha_liquidacion) AS fecha_liquidacion
  FROM silver_liquidacion_psp
  GROUP BY id_psp, _archivo_origen
)
SELECT
  coalesce(d.id_psp, t.id_psp)                                  AS id_psp,
  coalesce(d._archivo_origen, t._archivo_origen)                AS archivo,
  coalesce(d.fecha_liquidacion, t.fecha_liquidacion)            AS fecha_liquidacion,
  coalesce(d.registros_detalle, 0)                              AS registros_detalle,
  d.suma_detalle,
  t.total_declarado                                             AS total_trailer,
  coalesce(d.suma_detalle, 0) - coalesce(t.total_declarado, 0)  AS diferencia,
  CASE
    WHEN t._archivo_origen IS NULL                THEN 'SIN_TRAILER'
    WHEN d._archivo_origen IS NULL                THEN 'SIN_DETALLE'
    WHEN d.suma_detalle = t.total_declarado       THEN 'OK'
    ELSE 'DESCUADRADO'
  END AS estado
FROM detalle d
FULL OUTER JOIN silver_liquidacion_trailer t
  ON t.id_psp = d.id_psp AND t._archivo_origen = d._archivo_origen
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Control de calidad por fuente

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_calidad_datos
COMMENT 'Filas por fuente en cada etapa. duplicados = bronze - estructurales - cuarentena - silver.'
AS
WITH conteos AS (
  SELECT 'ventas' AS fuente,
         (SELECT COUNT(*) FROM bronze_ventas)            AS filas_bronze,
         0                                               AS filas_estructurales,
         (SELECT COUNT(*) FROM silver_cuarentena_ventas) AS filas_cuarentena,
         (SELECT COUNT(*) FROM silver_trx_cliente)       AS filas_silver
  UNION ALL
  SELECT 'eventos_mp',
         (SELECT COUNT(*) FROM bronze_eventos_mp), 0,
         (SELECT COUNT(*) FROM silver_cuarentena_eventos_psp WHERE id_psp = 1),
         (SELECT COUNT(*) FROM silver_evento_psp WHERE id_psp = 1)
  UNION ALL
  SELECT 'eventos_prisma',
         (SELECT COUNT(*) FROM bronze_eventos_prisma), 0,
         (SELECT COUNT(*) FROM silver_cuarentena_eventos_psp WHERE id_psp = 2),
         (SELECT COUNT(*) FROM silver_evento_psp WHERE id_psp = 2)
  UNION ALL
  SELECT 'liquidaciones',
         (SELECT COUNT(*) FROM bronze_liquidaciones_mp) + (SELECT COUNT(*) FROM bronze_liquidaciones_prisma),
         (SELECT COUNT(*) FROM silver_liquidacion_trailer),
         (SELECT COUNT(*) FROM silver_cuarentena_liquidaciones),
         (SELECT COUNT(*) FROM silver_liquidacion_psp)
  UNION ALL
  SELECT 'banco',
         (SELECT COUNT(*) FROM bronze_banco), 0,
         (SELECT COUNT(*) FROM silver_cuarentena_banco),
         (SELECT COUNT(*) FROM silver_mov_bancario)
)
SELECT
  fuente,
  filas_bronze,
  filas_estructurales,
  filas_cuarentena,
  filas_bronze - filas_estructurales - filas_cuarentena - filas_silver AS duplicados_removidos,
  filas_silver,
  round(100 * filas_silver / filas_bronze, 2)                          AS pct_aprovechado
FROM conteos
""")

# COMMAND ----------

# MAGIC %md ## Resumen de la tarea

# COMMAND ----------

display(spark.sql("SELECT capa, motivo, COUNT(*) AS partidas, SUM(monto) AS monto FROM gold_no_conciliadas GROUP BY ALL ORDER BY monto DESC"))
display(spark.table("gold_kpi_psp"))
display(spark.table("gold_calidad_datos").orderBy("fuente"))
