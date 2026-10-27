# 1. Datos

Esta sección describe el caso de estudio, el criterio de elección de los datos, el procedimiento de generación, el esquema de entrada de cada fuente y los problemas inyectados deliberadamente para evaluar el pipeline.

## Criterio de elección

El caso de estudio corresponde a la conciliación de pagos de una empresa que comercializa productos digitales y cobra a través de Mercado Pago y Prisma. Su elección se fundamenta en tres motivos:

1. **Pertinencia respecto de la arquitectura propuesta.** Se trata de un problema de integración de fuentes heterogéneas, que es precisamente el tipo de problema que una arquitectura medallion busca resolver. Intervienen cuatro sistemas (plataforma de ventas, dos PSP y entidad bancaria) que registran los mismos flujos de fondos con formatos, claves y tiempos diferentes.
2. **Disponibilidad de un modelo validado.** El modelo de negocio había sido diseñado y validado previamente en el Data Warehouse de la materia Almacenes de Datos ([conciliacion-pagos-digitales](https://github.com/SantinoCantisano/conciliacion-pagos-digitales)). Dicho modelo se reutiliza como base, y el aporte del presente trabajo reside en la ingesta desde archivos crudos, la capa de calidad y la orquestación en Databricks.
3. **Verificabilidad de los resultados.** Al tratarse de datos sintéticos, la cantidad de casos de cada tipo se conoce de antemano, lo cual permite contrastarla de manera directa con lo detectado por el pipeline.

Se optó por datos sintéticos debido a que los datos reales de pagos son confidenciales, contienen información personal y financiera, y su obtención no resulta factible fuera del ámbito de una empresa del sector. En consecuencia, el repositorio no contiene datos sensibles y, adicionalmente, se dispone de la respuesta correcta para cada caso. La principal desventaja es que los datos carecen de la variabilidad impredecible propia de los datos reales. Para mitigarla, los formatos y los errores se modelaron a partir de problemas habituales en integraciones con PSP: reenvío de reportes, montos expresados con formato local, columnas que se incorporan a lo largo del tiempo y registros de control incluidos dentro de los archivos.

## Generación de los datos

Los datos se generan mediante el script `generador/generar_datos.py`, desarrollado en Python estándar y sin dependencias externas. Sus características principales son las siguientes:

- La base de negocio emplea las mismas fórmulas que el script `02_populate.sql` del Data Warehouse original: 3.000 órdenes correspondientes a enero de 2026, 500 usuarios, 16 productos, 9 medios de pago y 2 PSP. Por este motivo, los montos obtenidos en la capa Gold coinciden con los del trabajo anterior ([ver](04-resultados.md#paridad-con-el-dw-original)).
- Los errores de calidad se incorporan en cantidades fijas y con una semilla fija (2026), de modo que cada ejecución produce exactamente los mismos archivos.
- El script registra en `_control/ground_truth.jsonl` la totalidad de los casos inyectados.
- El parámetro `--operaciones N` permite escalar el volumen de forma proporcional; la prueba con 500.000 órdenes se detalla en [04-resultados.md](04-resultados.md#escala).

## Fuentes y esquema de entrada

Se genera un archivo por día y por fuente (en el caso de la entidad bancaria, uno por cuenta). El conjunto completo comprende 253 archivos y 2,6 MB.

### Plataforma de ventas — `ventas/ventas_yyyymmdd.json`

Formato JSON Lines con estructura anidada y marca temporal en hora local.

```json
{"order_id": 100011, "created_at": "2026-01-20T19:23:23-03:00", "user_id": 122,
 "channel": "api", "status": "approved", "currency": "ARS",
 "items": [{"sku": "SRV-API-02", "quantity": 3, "unit_price": 2007.0}],
 "total_amount": 2007.0, "discount": 40.14,
 "payment": {"provider": "prisma", "reference": "REF-2-100011"},
 "coupon_code": null}
```

El campo `payment.reference` es el identificador que la plataforma envía al PSP. Dado que el PSP desconoce el `order_id` de la plataforma, el cruce entre ambas fuentes se realiza a través de esta referencia.

### Mercado Pago — `eventos_mp/mp_events_yyyymmdd.csv`

Archivo CSV delimitado por comas, con columnas en inglés, montos con punto decimal y marca temporal en UTC.

```
id,date_created,operation_id,external_reference,event_type,payment_method_id,currency_id,gross_amount,net_amount,fee_amount,tax_amount,chargeback_id,installments
200081,2026-01-20T19:30:38.000Z,500042,REF-1-100042,capture,visa,ARS,6254.00,5649.38,499.69,104.93,,3
```

### Prisma — `eventos_prisma/prisma_movimientos_yyyymmdd.csv`

Archivo CSV delimitado por punto y coma, con columnas en castellano, fechas en formato dd/mm/yyyy y montos con formato `12.345,67`. Emplea códigos de evento propios (`AUT`, `CAP`, `DEV`, `CTC`) y una nomenclatura propia para los medios de pago (por ejemplo, `VISA CREDITO`).

```
id_evento;fecha_mov;nro_operacion;ref_comercio;tipo_mov;medio;moneda;importe_bruto;importe_neto;arancel;iva_arancel;id_contracargo
200033;05/01/2026;500017;REF-2-100017;CAP;MODO;ARS;2.829,00;2.606,49;183,89;38,62;
```

### Liquidaciones — `liquidaciones/{mp,prisma}/…_yyyymmdd.csv`

Un archivo por PSP y por fecha de liquidación, cada uno con el formato propio de su emisor. Contiene registros de detalle (`D`) y un registro de cierre o *trailer* (`T`) con el total del archivo, conforme a la estructura habitual de los archivos de liquidación.

```
tipo_registro;id_liquidacion;nro_lote;fecha_liquidacion;id_evento;tipo_mov;importe_liquidado;arancel;retenciones;contracargos
D;802903;LOTE-PR-20260105-02;05/01/2026;205849;CAP;16.524,53;1.172,15;90,17;0,00
T;;;05/01/2026;;;1.862.826,01;;;
```

### Entidad bancaria — `banco/extracto_<cuenta>_yyyymmdd.csv`

Extracto de cada cuenta recaudadora, con débitos y créditos en columnas separadas, y con fecha de operación y fecha valor. Un mismo movimiento puede acreditar varios lotes, los cuales se identifican únicamente en el texto del concepto.

```
fecha_operacion;fecha_valor;nro_movimiento;concepto;debito;credito;saldo;cuenta
31/01/2026;30/01/2026;900056;ACREDITACION MERCADO PAGO LOTES: LOTE-MP-20260130-01,LOTE-MP-20260130-02;;1.668.799,21;46.730.921,55;CTA-MP-001
31/01/2026;31/01/2026;900072;COMISION MANTENIMIENTO CUENTA;15.000,00;;46.715.921,55;CTA-MP-001
```

### Datos maestros — `maestros/*.csv`

Comprenden productos, usuarios (con email y documento seudonimizados mediante SHA-256), canales, estados de pago, medios de pago, PSP (con su plazo de liquidación y cuenta bancaria asociada), tipos de evento con el código que utiliza cada PSP, y la tabla de equivalencias entre el código de medio de pago de cada PSP y el identificador interno `id_medio`.

## Errores de calidad y discrepancias de conciliación

El diseño distingue deliberadamente dos categorías de problemas:

| | Errores de calidad | Discrepancias de conciliación |
|---|---|---|
| Definición | Problemas de formato o de transmisión | Datos correctamente formados que no coinciden entre capas |
| Ejemplo | Monto `"N/A"`, registro reenviado | Venta aprobada sin evento en el PSP |
| Capa responsable | Silver, que los corrige o descarta | Gold, que los detecta y clasifica |
| Forma de inyección | Filas adicionales o copias | Idéntica a la del DW original |

Si la capa Silver eliminara, por ejemplo, una captura sin venta asociada, suprimiría precisamente aquello que la conciliación debe identificar. Por este motivo, los errores de calidad se inyectaron como filas adicionales, con identificadores fuera del rango real y sin contraparte en las demás fuentes. De este modo, su descarte no altera ningún caso de conciliación.

### Errores de calidad inyectados

| Fuente | Problema | Cantidad | Tratamiento en Silver |
|---|---|---:|---|
| Ventas | Reenvío de la misma orden en el archivo del día siguiente | 45 | Deduplicación por `id_trx` |
| Ventas | `total_amount` con valor `"N/A"` o vacío | 12 | Cuarentena (`MONTO_INVALIDO`) |
| Ventas | `user_id` nulo | 8 | Cuarentena (`USUARIO_AUSENTE`) |
| Ventas | `status` con valor `"APPROVD"` | 6 | Cuarentena (`ESTADO_DESCONOCIDO`) |
| Ventas | `status` con mayúsculas o espacios (`" Approved "`) | 309 | Normalización, sin descarte |
| Ventas | Nueva columna `coupon_code` a partir del 16/01 | — | Evolución de esquema en Bronze |
| Mercado Pago | Reenvío de eventos | 30 | Deduplicación por `(id_psp, id_evento)` |
| Mercado Pago | `gross_amount` vacío | 10 | Cuarentena (`MONTO_BRUTO_INVALIDO`) |
| Mercado Pago | Nueva columna `installments` a partir del 16/01 | — | Evolución de esquema en Bronze |
| Prisma | Reenvío de eventos | 25 | Deduplicación |
| Prisma | Fechas inexistentes (`31/02/2026`, `00/01/2026`) | 6 | Cuarentena (`FECHA_INVALIDA`) |
| Prisma | Montos con formato `12.345,67` y fechas dd/mm/yyyy | todas | Conversión de formato |
| Liquidaciones | Registro trailer en cada archivo | 71 | Separación y uso como control de integridad |

### Discrepancias de conciliación (heredadas del DW original)

| Capa | Caso | Cantidad |
|---|---|---:|
| Operativa | Fantasma cliente: venta aprobada sin evento en el PSP | 90 |
| Operativa | Diferencia de monto: el PSP capturó el 95% del monto de la orden | 90 |
| Operativa | Fantasma PSP: captura con una referencia inexistente en la plataforma | 100 |
| Financiera | Captura sin liquidar | 37 |
| Bancaria | Lote nunca acreditado | 2 |
| Bancaria | Lote acreditado al día siguiente (fin de mes) | 38 |
| Bancaria | Movimiento bancario sin lote asociado (comisión de mantenimiento) | 2 |

El último caso no formaba parte del Data Warehouse original. Se incorporó dado que el extracto de una cuenta real incluye movimientos que no corresponden a acreditaciones de PSP, y la conciliación debe ponerlos de manifiesto.
