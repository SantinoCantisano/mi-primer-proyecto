# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Construcción de Silver
# MAGIC
# MAGIC Silver tipa, normaliza entre fuentes, valida y deduplica. Para cada entidad:
# MAGIC
# MAGIC 1. Una vista de staging asigna tipos, traduce los códigos de cada PSP a la codificación interna y calcula `motivo_calidad`: la primera regla incumplida, evaluada desde el formato hacia la integridad referencial.
# MAGIC 2. Los registros válidos se deduplican (se conserva el primero recibido) y se integran a la tabla Silver con `MERGE`, que inserta únicamente las claves nuevas. Reejecutar la tarea no modifica la tabla.
# MAGIC 3. Los registros inválidos se integran a la tabla de cuarentena correspondiente, junto con el motivo.
# MAGIC
# MAGIC Silver trata exclusivamente los errores de calidad. Las discrepancias de conciliación (fantasmas, diferencias de monto, lotes sin acreditar) se conservan sin modificaciones para que la capa Gold las detecte.
# MAGIC
# MAGIC Se utilizan siempre las variantes `try_*` de conversión: una conversión convencional (`CAST`) sobre un valor inválido provocaría la falla de la tarea completa a causa de una única fila.

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

for t in ["bronze_ventas", "bronze_eventos_mp", "bronze_eventos_prisma",
          "bronze_liquidaciones_mp", "bronze_liquidaciones_prisma", "bronze_banco"]:
    assert spark.catalog.tableExists(t), f"Falta {t}. Ejecutar la tarea 01."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensiones
# MAGIC Los maestros se leen completos en cada ejecución y reemplazan a la versión anterior, sin conservar historial. `silver_dim_fecha` reemplaza a las tablas `Mes` y `Fecha` y a la función `semana_del_mes()` del Data Warehouse original.

# COMMAND ----------

def leer_maestro(nombre):
    (spark.read.option("header", "true").option("inferSchema", "false")
        .csv(f"{raw}/maestros/{nombre}.csv")
        .createOrReplaceTempView(f"_m_{nombre}"))

for m in ["productos", "usuarios", "canales", "estados_pago", "medios_pago",
          "mapeo_medios_psp", "psp", "tipos_evento"]:
    leer_maestro(m)

spark.sql("""
CREATE OR REPLACE TABLE silver_dim_fecha AS
SELECT fecha,
       year(fecha) * 100 + month(fecha)  AS id_mes,
       date_format(fecha, 'yyyy-MM')     AS mes,
       ((day(fecha) - 1) DIV 7) + 1      AS semana_del_mes,
       CASE dayofweek(fecha)
         WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes' WHEN 4 THEN 'Miercoles'
         WHEN 5 THEN 'Jueves'  WHEN 6 THEN 'Viernes' ELSE 'Sabado'
       END                               AS dia_semana
FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2026-02-28', INTERVAL 1 DAY)) AS fecha)
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_dim_producto AS
SELECT sku, nombre, CAST(id_categoria AS INT) AS id_categoria, categoria AS categoria_nombre, iso_moneda
FROM _m_productos
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_dim_usuario AS
SELECT CAST(id_usuario AS BIGINT) AS id_usuario, hash_email, hash_doc,
       CAST(edad AS SMALLINT) AS edad, CAST(id_segmento AS INT) AS id_segmento,
       segmento AS segmento_nombre, grupo_etario
FROM _m_usuarios
""")

spark.sql("CREATE OR REPLACE TABLE silver_dim_canal AS SELECT CAST(id_canal AS INT) AS id_canal, codigo, tipo_canal FROM _m_canales")
spark.sql("CREATE OR REPLACE TABLE silver_dim_estado_pago AS SELECT CAST(id_estado AS INT) AS id_estado, codigo, estado_pago FROM _m_estados_pago")

spark.sql("""
CREATE OR REPLACE TABLE silver_dim_medio_pago AS
SELECT CAST(id_medio AS INT) AS id_medio, nombre, marca, CAST(id_tipo AS INT) AS id_tipo, tipo_medio,
       CAST(id_comercio AS INT) AS id_comercio, razon_social, cuit
FROM _m_medios_pago
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_map_medio_psp AS
SELECT CAST(id_psp AS INT) AS id_psp, codigo_psp, CAST(id_medio AS INT) AS id_medio
FROM _m_mapeo_medios_psp
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_dim_psp AS
SELECT CAST(id_psp AS INT) AS id_psp, nombre, codigo, pais_op,
       CAST(plazo_liquidacion_dias AS INT) AS plazo_liquidacion_dias, cuenta_bancaria
FROM _m_psp
""")

spark.sql("CREATE OR REPLACE TABLE silver_dim_tipo_evento AS SELECT cod_evento, tipo_evento, codigo_mp, codigo_prisma FROM _m_tipos_evento")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ventas → `silver_trx_cliente`
# MAGIC Grano: una fila por orden (una orden contiene un único SKU). La fecha se toma de la porción de fecha del texto, que ya está expresada en hora local, para no depender de la zona horaria de la sesión.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _stg_ventas AS
WITH v AS (
  SELECT *, try_element_at(items, 1) AS linea FROM bronze_ventas
),
tipado AS (
  SELECT
    v.order_id                                         AS order_id_crudo,
    v.created_at                                       AS created_at_crudo,
    v.user_id                                          AS user_id_crudo,
    v.status                                           AS status_crudo,
    v.total_amount                                     AS total_amount_crudo,
    try_cast(v.order_id AS BIGINT)                     AS id_trx,
    try_to_timestamp(v.created_at)                     AS fecha_hora_trx,
    try_cast(substr(v.created_at, 1, 10) AS DATE)      AS fecha_trx_cliente,
    v.linea.sku                                        AS sku,
    try_cast(v.linea.quantity AS INT)                  AS cantidad_items,
    try_cast(v.user_id AS BIGINT)                      AS id_usuario,
    c.id_canal,
    e.id_estado,
    v.status <> lower(trim(v.status))                  AS estado_normalizado,
    try_cast(v.total_amount AS DECIMAL(14,2))          AS monto_orden,
    coalesce(try_cast(v.discount AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))) AS descuento,
    v.currency                                         AS iso_moneda,
    v.payment.provider                                 AS psp_codigo,
    v.payment.reference                                AS referencia_pago,
    p.sku IS NOT NULL                                  AS sku_en_catalogo,
    size(v.items)                                      AS lineas_en_orden,
    v._archivo_origen,
    v._fecha_ingesta
  FROM v
  LEFT JOIN silver_dim_canal       c ON c.codigo = lower(trim(v.channel))
  LEFT JOIN silver_dim_estado_pago e ON e.codigo = lower(trim(v.status))
  LEFT JOIN silver_dim_producto    p ON p.sku    = v.linea.sku
)
SELECT *,
  CASE
    WHEN id_trx IS NULL                        THEN 'ID_INVALIDO'
    WHEN fecha_trx_cliente IS NULL             THEN 'FECHA_INVALIDA'
    WHEN NOT coalesce(monto_orden > 0, false)  THEN 'MONTO_INVALIDO'
    WHEN id_usuario IS NULL                    THEN 'USUARIO_AUSENTE'
    WHEN id_estado IS NULL                     THEN 'ESTADO_DESCONOCIDO'
    WHEN id_canal IS NULL                      THEN 'CANAL_DESCONOCIDO'
    WHEN NOT sku_en_catalogo                   THEN 'SKU_DESCONOCIDO'
    WHEN referencia_pago IS NULL               THEN 'REFERENCIA_AUSENTE'
  END AS motivo_calidad
FROM tipado
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_trx_cliente (
  id_trx BIGINT, fecha_trx_cliente DATE, fecha_hora_trx TIMESTAMP, sku STRING, id_usuario BIGINT,
  id_canal INT, id_estado INT, monto_orden DECIMAL(14,2), cantidad_items INT, cantidad_ordenes INT,
  descuento DECIMAL(14,2), iso_moneda STRING, psp_codigo STRING, referencia_pago STRING,
  estado_crudo STRING, estado_normalizado BOOLEAN, lineas_en_orden INT,
  _archivo_origen STRING, _fecha_ingesta TIMESTAMP, procesado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _ventas_validas AS
SELECT id_trx, fecha_trx_cliente, fecha_hora_trx, sku, id_usuario, id_canal, id_estado, monto_orden,
       cantidad_items, 1 AS cantidad_ordenes, descuento, iso_moneda, psp_codigo, referencia_pago,
       status_crudo AS estado_crudo, estado_normalizado, lineas_en_orden,
       _archivo_origen, _fecha_ingesta, current_timestamp() AS procesado_en
FROM (
  SELECT *, row_number() OVER (PARTITION BY id_trx ORDER BY _fecha_ingesta, _archivo_origen) AS _rn
  FROM _stg_ventas
  WHERE motivo_calidad IS NULL
)
WHERE _rn = 1
""")

spark.sql("""
MERGE INTO silver_trx_cliente t USING _ventas_validas s ON t.id_trx = s.id_trx
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_cuarentena_ventas (
  order_id_crudo STRING, created_at_crudo STRING, user_id_crudo STRING, status_crudo STRING,
  total_amount_crudo STRING, motivo_calidad STRING, _archivo_origen STRING,
  _fecha_ingesta TIMESTAMP, rechazado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _ventas_rechazadas AS
SELECT order_id_crudo, created_at_crudo, user_id_crudo, status_crudo, total_amount_crudo,
       motivo_calidad, _archivo_origen, _fecha_ingesta, current_timestamp() AS rechazado_en
FROM _stg_ventas
WHERE motivo_calidad IS NOT NULL
""")

spark.sql("""
MERGE INTO silver_cuarentena_ventas t USING _ventas_rechazadas s
ON t._archivo_origen = s._archivo_origen AND t.order_id_crudo <=> s.order_id_crudo AND t.motivo_calidad = s.motivo_calidad
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Eventos de ambos PSP → `silver_evento_psp`
# MAGIC Cada PSP informa en su propio formato. La vista de staging lleva ambos reportes a un esquema común: Mercado Pago se convierte de UTC a hora de Buenos Aires antes de extraer la fecha, y los montos de Prisma (`12.345,67`) se convierten a decimal.
# MAGIC
# MAGIC Grano: una fila por evento (AUTH, CAPTURE, REFUND o CHBK). El evento no contiene el `id_trx` de la plataforma; el vínculo con la venta se resuelve en Gold a través de la referencia de pago, de modo que un evento recibido antes que su venta no queda desvinculado.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _stg_eventos AS
WITH unificado AS (
  SELECT
    1                                                    AS id_psp,
    m.id                                                 AS id_evento_crudo,
    m.date_created                                       AS fecha_cruda,
    m.event_type                                         AS tipo_evento_crudo,
    m.payment_method_id                                  AS medio_crudo,
    m.gross_amount                                       AS monto_bruto_crudo,
    try_cast(m.id AS BIGINT)                             AS id_evento_psp,
    try_to_timestamp(m.date_created)                     AS fecha_hora_evento,
    CAST(convert_timezone('UTC', 'America/Argentina/Buenos_Aires',
         try_cast(substr(m.date_created, 1, 19) AS TIMESTAMP_NTZ)) AS DATE) AS fecha_evento,
    try_cast(m.operation_id AS BIGINT)                   AS id_operacion_psp,
    m.external_reference,
    te.cod_evento,
    mm.id_medio,
    try_cast(m.gross_amount AS DECIMAL(14,2))            AS monto_bruto,
    try_cast(m.net_amount   AS DECIMAL(14,2))            AS monto_neto,
    coalesce(try_cast(m.fee_amount AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))) AS fee_psp,
    coalesce(try_cast(m.tax_amount AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))) AS impuestos,
    try_cast(m.chargeback_id AS BIGINT)                  AS id_contracargo,
    m._archivo_origen,
    m._fecha_ingesta
  FROM bronze_eventos_mp m
  LEFT JOIN silver_dim_tipo_evento te ON te.codigo_mp = lower(trim(m.event_type))
  LEFT JOIN silver_map_medio_psp   mm ON mm.id_psp = 1 AND mm.codigo_psp = lower(trim(m.payment_method_id))

  UNION ALL

  SELECT
    2,
    p.id_evento,
    p.fecha_mov,
    p.tipo_mov,
    p.medio,
    p.importe_bruto,
    try_cast(p.id_evento AS BIGINT),
    CAST(NULL AS TIMESTAMP),
    CAST(try_to_timestamp(p.fecha_mov, 'dd/MM/yyyy') AS DATE),
    try_cast(p.nro_operacion AS BIGINT),
    p.ref_comercio,
    te.cod_evento,
    mm.id_medio,
    try_cast(replace(replace(p.importe_bruto, '.', ''), ',', '.') AS DECIMAL(14,2)),
    try_cast(replace(replace(p.importe_neto,  '.', ''), ',', '.') AS DECIMAL(14,2)),
    coalesce(try_cast(replace(replace(p.arancel,     '.', ''), ',', '.') AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))),
    coalesce(try_cast(replace(replace(p.iva_arancel, '.', ''), ',', '.') AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))),
    try_cast(p.id_contracargo AS BIGINT),
    p._archivo_origen,
    p._fecha_ingesta
  FROM bronze_eventos_prisma p
  LEFT JOIN silver_dim_tipo_evento te ON te.codigo_prisma = upper(trim(p.tipo_mov))
  LEFT JOIN silver_map_medio_psp   mm ON mm.id_psp = 2 AND mm.codigo_psp = upper(trim(p.medio))
)
SELECT *,
  CASE
    WHEN id_evento_psp IS NULL                                   THEN 'ID_INVALIDO'
    WHEN fecha_evento IS NULL                                    THEN 'FECHA_INVALIDA'
    WHEN monto_bruto IS NULL                                     THEN 'MONTO_BRUTO_INVALIDO'
    WHEN monto_neto IS NULL                                      THEN 'MONTO_NETO_INVALIDO'
    WHEN cod_evento IS NULL                                      THEN 'TIPO_EVENTO_DESCONOCIDO'
    WHEN id_medio IS NULL                                        THEN 'MEDIO_DESCONOCIDO'
    WHEN id_operacion_psp IS NULL OR external_reference IS NULL  THEN 'OPERACION_AUSENTE'
  END AS motivo_calidad
FROM unificado
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_evento_psp (
  id_psp INT, id_evento_psp BIGINT, fecha_evento DATE, fecha_hora_evento TIMESTAMP, id_medio INT,
  id_operacion_psp BIGINT, external_reference STRING, cod_evento STRING, id_contracargo BIGINT,
  monto_bruto DECIMAL(14,2), monto_neto DECIMAL(14,2), fee_psp DECIMAL(14,2), impuestos DECIMAL(14,2),
  cantidad_eventos INT, _archivo_origen STRING, _fecha_ingesta TIMESTAMP, procesado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _eventos_validos AS
SELECT id_psp, id_evento_psp, fecha_evento, fecha_hora_evento, id_medio, id_operacion_psp,
       external_reference, cod_evento, id_contracargo, monto_bruto, monto_neto, fee_psp, impuestos,
       1 AS cantidad_eventos, _archivo_origen, _fecha_ingesta, current_timestamp() AS procesado_en
FROM (
  SELECT *, row_number() OVER (PARTITION BY id_psp, id_evento_psp ORDER BY _fecha_ingesta, _archivo_origen) AS _rn
  FROM _stg_eventos
  WHERE motivo_calidad IS NULL
)
WHERE _rn = 1
""")

spark.sql("""
MERGE INTO silver_evento_psp t USING _eventos_validos s
ON t.id_psp = s.id_psp AND t.id_evento_psp = s.id_evento_psp
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_cuarentena_eventos_psp (
  id_psp INT, id_evento_crudo STRING, fecha_cruda STRING, tipo_evento_crudo STRING, medio_crudo STRING,
  monto_bruto_crudo STRING, motivo_calidad STRING, _archivo_origen STRING,
  _fecha_ingesta TIMESTAMP, rechazado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _eventos_rechazados AS
SELECT id_psp, id_evento_crudo, fecha_cruda, tipo_evento_crudo, medio_crudo, monto_bruto_crudo,
       motivo_calidad, _archivo_origen, _fecha_ingesta, current_timestamp() AS rechazado_en
FROM _stg_eventos
WHERE motivo_calidad IS NOT NULL
""")

spark.sql("""
MERGE INTO silver_cuarentena_eventos_psp t USING _eventos_rechazados s
ON t._archivo_origen = s._archivo_origen AND t.id_evento_crudo <=> s.id_evento_crudo AND t.motivo_calidad = s.motivo_calidad
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_operacion_psp AS
SELECT DISTINCT id_psp, id_operacion_psp, external_reference FROM silver_evento_psp
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_contracargo AS
SELECT id_contracargo, id_psp, -monto_bruto AS monto_contracargo, fee_psp AS fee_contracargo
FROM silver_evento_psp
WHERE cod_evento = 'CHBK' AND id_contracargo IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Liquidaciones → `silver_liquidacion_psp` y `silver_liquidacion_trailer`
# MAGIC Los registros de detalle (`D`) se integran como hechos. El registro de cierre (`T`) no representa una operación: se almacena por separado y Gold lo utiliza para controlar la integridad de cada archivo.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _stg_liquidaciones AS
WITH unificado AS (
  SELECT
    1                                                    AS id_psp,
    upper(trim(m.record_type))                           AS tipo_registro,
    m.settlement_id                                      AS id_liquidacion_crudo,
    try_cast(m.settlement_id AS BIGINT)                  AS id_liquidacion,
    m.batch_id                                           AS id_lote_liquidacion,
    try_cast(m.settlement_date AS DATE)                  AS fecha_liquidacion,
    try_cast(m.source_event_id AS BIGINT)                AS id_evento_psp,
    te.cod_evento,
    try_cast(m.settled_amount AS DECIMAL(16,2))          AS monto_liquidado,
    coalesce(try_cast(m.fee_amount         AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))) AS fees_liquid,
    coalesce(try_cast(m.withholding_amount AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))) AS retenciones,
    coalesce(try_cast(m.chargeback_amount  AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))) AS contracargos,
    m._archivo_origen,
    m._fecha_ingesta
  FROM bronze_liquidaciones_mp m
  LEFT JOIN silver_dim_tipo_evento te ON te.codigo_mp = lower(trim(m.transaction_type))

  UNION ALL

  SELECT
    2,
    upper(trim(p.tipo_registro)),
    p.id_liquidacion,
    try_cast(p.id_liquidacion AS BIGINT),
    p.nro_lote,
    CAST(try_to_timestamp(p.fecha_liquidacion, 'dd/MM/yyyy') AS DATE),
    try_cast(p.id_evento AS BIGINT),
    te.cod_evento,
    try_cast(replace(replace(p.importe_liquidado, '.', ''), ',', '.') AS DECIMAL(16,2)),
    coalesce(try_cast(replace(replace(p.arancel,      '.', ''), ',', '.') AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))),
    coalesce(try_cast(replace(replace(p.retenciones,  '.', ''), ',', '.') AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))),
    coalesce(try_cast(replace(replace(p.contracargos, '.', ''), ',', '.') AS DECIMAL(14,2)), CAST(0 AS DECIMAL(14,2))),
    p._archivo_origen,
    p._fecha_ingesta
  FROM bronze_liquidaciones_prisma p
  LEFT JOIN silver_dim_tipo_evento te ON te.codigo_prisma = upper(trim(p.tipo_mov))
)
SELECT *,
  CASE
    WHEN tipo_registro <> 'D'            THEN NULL
    WHEN id_liquidacion IS NULL          THEN 'ID_INVALIDO'
    WHEN fecha_liquidacion IS NULL       THEN 'FECHA_INVALIDA'
    WHEN id_lote_liquidacion IS NULL     THEN 'LOTE_AUSENTE'
    WHEN monto_liquidado IS NULL         THEN 'MONTO_INVALIDO'
    WHEN cod_evento IS NULL              THEN 'TIPO_EVENTO_DESCONOCIDO'
  END AS motivo_calidad
FROM unificado
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_liquidacion_psp (
  id_psp INT, id_liquidacion BIGINT, fecha_liquidacion DATE, cod_evento STRING, id_evento_psp BIGINT,
  id_lote_liquidacion STRING, monto_liquidado DECIMAL(14,2), fees_liquid DECIMAL(14,2),
  retenciones DECIMAL(14,2), contracargos DECIMAL(14,2),
  _archivo_origen STRING, _fecha_ingesta TIMESTAMP, procesado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _liquidaciones_validas AS
SELECT id_psp, id_liquidacion, fecha_liquidacion, cod_evento, id_evento_psp, id_lote_liquidacion,
       CAST(monto_liquidado AS DECIMAL(14,2)) AS monto_liquidado, fees_liquid, retenciones, contracargos,
       _archivo_origen, _fecha_ingesta, current_timestamp() AS procesado_en
FROM (
  SELECT *, row_number() OVER (PARTITION BY id_psp, id_liquidacion ORDER BY _fecha_ingesta, _archivo_origen) AS _rn
  FROM _stg_liquidaciones
  WHERE tipo_registro = 'D' AND motivo_calidad IS NULL
)
WHERE _rn = 1
""")

spark.sql("""
MERGE INTO silver_liquidacion_psp t USING _liquidaciones_validas s
ON t.id_psp = s.id_psp AND t.id_liquidacion = s.id_liquidacion
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_cuarentena_liquidaciones (
  id_psp INT, id_liquidacion_crudo STRING, id_lote_liquidacion STRING, motivo_calidad STRING,
  _archivo_origen STRING, _fecha_ingesta TIMESTAMP, rechazado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _liquidaciones_rechazadas AS
SELECT id_psp, id_liquidacion_crudo, id_lote_liquidacion, motivo_calidad,
       _archivo_origen, _fecha_ingesta, current_timestamp() AS rechazado_en
FROM _stg_liquidaciones
WHERE tipo_registro = 'D' AND motivo_calidad IS NOT NULL
""")

spark.sql("""
MERGE INTO silver_cuarentena_liquidaciones t USING _liquidaciones_rechazadas s
ON t._archivo_origen = s._archivo_origen AND t.id_liquidacion_crudo <=> s.id_liquidacion_crudo AND t.motivo_calidad = s.motivo_calidad
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_liquidacion_trailer AS
SELECT id_psp, _archivo_origen, MAX(fecha_liquidacion) AS fecha_liquidacion,
       MAX(monto_liquidado) AS total_declarado
FROM _stg_liquidaciones
WHERE tipo_registro = 'T'
GROUP BY id_psp, _archivo_origen
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Banco → `silver_mov_bancario` y `silver_mov_bancario_lote`
# MAGIC El extracto informa débitos y créditos por separado; `importe` se expresa con signo. Un movimiento acredita varios lotes que solo se mencionan en el concepto, por lo que se extraen a una tabla puente que permite a Gold resolver el cruce mediante un join por igualdad.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW _stg_banco AS
WITH b AS (
  SELECT *,
    try_cast(replace(replace(credito, '.', ''), ',', '.') AS DECIMAL(16,2)) AS _credito,
    try_cast(replace(replace(debito,  '.', ''), ',', '.') AS DECIMAL(16,2)) AS _debito
  FROM bronze_banco
),
tipado AS (
  SELECT
    nro_movimiento                                                AS nro_movimiento_crudo,
    try_cast(nro_movimiento AS BIGINT)                            AS id_mov_banco,
    CAST(try_to_timestamp(fecha_operacion, 'dd/MM/yyyy') AS DATE) AS fecha_movimiento,
    CAST(try_to_timestamp(fecha_valor,     'dd/MM/yyyy') AS DATE) AS fecha_correspondiente,
    CASE WHEN _credito IS NULL AND _debito IS NULL THEN NULL
         ELSE CAST(coalesce(_credito, 0) - coalesce(_debito, 0) AS DECIMAL(16,2)) END AS importe,
    try_cast(replace(replace(saldo, '.', ''), ',', '.') AS DECIMAL(16,2)) AS saldo_post,
    trim(concepto)                                                AS descripcion,
    cuenta                                                        AS cuenta_bancaria,
    _archivo_origen,
    _fecha_ingesta
  FROM b
)
SELECT *,
  CASE
    WHEN id_mov_banco IS NULL       THEN 'ID_INVALIDO'
    WHEN fecha_movimiento IS NULL   THEN 'FECHA_INVALIDA'
    WHEN importe IS NULL            THEN 'IMPORTE_INVALIDO'
    WHEN cuenta_bancaria IS NULL    THEN 'CUENTA_AUSENTE'
  END AS motivo_calidad
FROM tipado
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_mov_bancario (
  cuenta_bancaria STRING, id_mov_banco BIGINT, fecha_movimiento DATE, fecha_correspondiente DATE,
  importe DECIMAL(16,2), saldo_post DECIMAL(16,2), cant_movimientos INT, descripcion STRING,
  _archivo_origen STRING, _fecha_ingesta TIMESTAMP, procesado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _banco_validos AS
SELECT cuenta_bancaria, id_mov_banco, fecha_movimiento, fecha_correspondiente, importe, saldo_post,
       1 AS cant_movimientos, descripcion, _archivo_origen, _fecha_ingesta, current_timestamp() AS procesado_en
FROM (
  SELECT *, row_number() OVER (PARTITION BY cuenta_bancaria, id_mov_banco ORDER BY _fecha_ingesta, _archivo_origen) AS _rn
  FROM _stg_banco
  WHERE motivo_calidad IS NULL
)
WHERE _rn = 1
""")

spark.sql("""
MERGE INTO silver_mov_bancario t USING _banco_validos s
ON t.cuenta_bancaria = s.cuenta_bancaria AND t.id_mov_banco = s.id_mov_banco
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_cuarentena_banco (
  nro_movimiento_crudo STRING, cuenta_bancaria STRING, motivo_calidad STRING,
  _archivo_origen STRING, _fecha_ingesta TIMESTAMP, rechazado_en TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW _banco_rechazados AS
SELECT nro_movimiento_crudo, cuenta_bancaria, motivo_calidad, _archivo_origen, _fecha_ingesta,
       current_timestamp() AS rechazado_en
FROM _stg_banco
WHERE motivo_calidad IS NOT NULL
""")

spark.sql("""
MERGE INTO silver_cuarentena_banco t USING _banco_rechazados s
ON t._archivo_origen = s._archivo_origen AND t.nro_movimiento_crudo <=> s.nro_movimiento_crudo AND t.motivo_calidad = s.motivo_calidad
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_mov_bancario_lote AS
SELECT m.cuenta_bancaria, m.id_mov_banco, trim(lote) AS id_lote_liquidacion
FROM silver_mov_bancario m
LATERAL VIEW explode(split(regexp_extract(m.descripcion, 'LOTES: (.*)$', 1), ',')) l AS lote
WHERE m.descripcion LIKE '%LOTES: %' AND trim(lote) <> ''
""")

# COMMAND ----------

# MAGIC %md ## 6. Resumen de la tarea

# COMMAND ----------

display(spark.sql("""
SELECT 'silver_trx_cliente' AS tabla, COUNT(*) AS filas FROM silver_trx_cliente
UNION ALL SELECT 'silver_cuarentena_ventas', COUNT(*) FROM silver_cuarentena_ventas
UNION ALL SELECT 'silver_evento_psp', COUNT(*) FROM silver_evento_psp
UNION ALL SELECT 'silver_cuarentena_eventos_psp', COUNT(*) FROM silver_cuarentena_eventos_psp
UNION ALL SELECT 'silver_liquidacion_psp', COUNT(*) FROM silver_liquidacion_psp
UNION ALL SELECT 'silver_liquidacion_trailer', COUNT(*) FROM silver_liquidacion_trailer
UNION ALL SELECT 'silver_cuarentena_liquidaciones', COUNT(*) FROM silver_cuarentena_liquidaciones
UNION ALL SELECT 'silver_mov_bancario', COUNT(*) FROM silver_mov_bancario
UNION ALL SELECT 'silver_mov_bancario_lote', COUNT(*) FROM silver_mov_bancario_lote
UNION ALL SELECT 'silver_cuarentena_banco', COUNT(*) FROM silver_cuarentena_banco
"""))

display(spark.sql("""
SELECT 'ventas' AS fuente, motivo_calidad, COUNT(*) AS filas FROM silver_cuarentena_ventas GROUP BY ALL
UNION ALL
SELECT CASE id_psp WHEN 1 THEN 'eventos_mp' ELSE 'eventos_prisma' END, motivo_calidad, COUNT(*)
FROM silver_cuarentena_eventos_psp GROUP BY ALL
ORDER BY fuente, motivo_calidad
"""))
