# Práctica 1 — Ingesta y capa Bronze

**Nombre:** Santino Cantisano
**student_id:** santino_cantisano

## Diagnóstico de calidad (bronze_transactions)

| Métrica | Valor |
|---|---:|
| Filas totales | 50011 |
| IDs distintos | 50000 |
| IDs duplicados | 11 |
| Importes no convertibles a DECIMAL(12,2) | 52 |

## Observaciones sobre formatos

**1. CSV y JSON: el esquema se adivina, no viene con el dato.**
CSV es texto plano. Sin `inferSchema`, Spark lee todas las columnas como `string`. Con `inferSchema=true`, Spark hace una pasada extra sobre los datos para deducir los tipos, así que la lectura tarda más. Además, el resultado depende del contenido, ya que alcanza con que un solo valor de `amount` no sea numérico (por ejemplo un texto, un vacío o un formato raro) para que la columna entera quede como `string`. Por eso la inferencia es inestable: el mismo pipeline puede producir tipos distintos según el lote. JSON también infiere leyendo los datos. Soporta estructuras anidadas, pero los campos que faltan en algunos registros aparecen como `null` y el esquema puede variar entre archivos.

**2. Parquet: formato columnar, tipado y autodescriptivo.**
El esquema y los tipos están guardados en el propio archivo, así que no hace falta inferir nada. Al ser columnar y comprimido, Spark lee solo las columnas que necesita. En el `EXPLAIN FORMATTED` se ve que el `ReadSchema` del scan incluye únicamente `payment_channel` (column pruning). Su límite es que un directorio Parquet es solo una colección de archivos: no tiene transacciones, ni historial, ni control sobre escrituras concurrentes.

**3. Delta: Parquet más un log de transacciones.**
Delta guarda los datos en Parquet y les agrega un `_delta_log`. `DESCRIBE DETAIL` muestra metadatos de la tabla que un directorio Parquet no expone, siendo estos formato, ubicación, cantidad de archivos, tamaño y columnas de partición. `DESCRIBE HISTORY` registra cada versión con su operación, su timestamp y sus parámetros (por ejemplo, el overwrite), lo que permite auditar y hacer time travel. Además, Delta aplica *schema enforcement*: por eso tuvimos que usar `overwriteSchema=True` para reescribir. 

## Las cinco V en este caso

- **Volumen:** el widget `scale` (test/small/demo) controla el tamaño. Transacciones y eventos son las fuentes que crecen con la actividad del negocio, y procesarlas con Spark distribuido permite escalar sin cambiar el código.
- **Velocidad:** los eventos (JSON) representan actividad de alta frecuencia que en un caso real llega de forma continua. Hoy ingerimos en batch, pero `_ingested_at` y el historial de Delta sientan la base para cargas incrementales.
- **Variedad:** tenemos tres formatos con distinto grado de estructura. CSV es texto sin tipos, JSON es semiestructurado y Parquet es columnar y tipado. Bronze los unifica como tablas Delta sin perder el origen (`_source`, `_source_file`).
- **Veracidad:** el diagnóstico encontró 11 IDs duplicados y 52 importes inválidos. El dato de origen no es confiable tal como llega. Bronze lo conserva y lo mide para que Silver pueda limpiarlo de forma trazable.
- **Valor:** el valor aparece al cruzar clientes, productos, transacciones y eventos, por ejemplo para ver ventas por canal, el comportamiento de los clientes o la conversión de eventos a compras. Bronze no genera valor de negocio por sí solo, pero lo habilita, ya que sin trazabilidad no se puede confiar en las métricas que se construyan encima.