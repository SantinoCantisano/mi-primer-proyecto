# Entrega práctica 4 — Streaming

- **Nombre:** Santino Cantisano
- **student_id:** santino_cantisano
- **Escala:** small

## Log y offsets

**1.** En la cola quedaron 2, y en el log, 5. En una cola tradicional, leer un mensaje lo borra. En un log no se modifica al leerse.

**2.** Antifraude había leído 180 y reportes 0. El atraso de reportes es independiente de antifraude, los dos reciben los mensajes sin molestarse entre sí por cómo están configurados los checkpoints.

**3.** El offset es una forma de registrar en qué punto quedó la corrida y retomar desde allí en caso de ser necesario. Cada consumidor lo guarda en su propio checkpoint.

**4.** El replay leyó 280 mensajes. Kappa requiere releer el log desde el principio, funcionando con un solo código de streaming. En Lambda, en cambio, hay dos sistemas en paralelo: un batch preciso y una capa de streaming rápida, con la misma lógica escrita dos veces.

## Ingesta y enriquecimiento

**5.** Bronze procesó las siguientes filas:

| llegada | filas en el archivo | nuevas en Bronze | nuevas en Silver |
|---|---|---|---|
| stream_001 | 60 | 60 | 60 |
| stream_002 | 120 | 120 | 120 |
| stream_003 | 40 | 40 | 40 |
| stream_004 | 40 | 40 | 40 |
| stream_005 | 20 | 20 | 20 |

Al reejecutar sin archivos nuevos se procesaron 0 filas. Lo evita el checkpoint de Auto Loader, que recuerda qué archivos ya leyó.

**6.** Los motivos que aparecieron en cuarentena en stream_002 fueron INVALID_AMOUNT y UNKNOWN_CUSTOMER. Bronze guarda el texto original ya que es la capa en donde los reportes llegan sin refinarse, a diferencia de Silver, así se pueden reprocesar si hay un error.

**7.** Agregó country desde silver_customers y category desde silver_products. Es un join stream-tabla, porque el stream se cruza con tablas estáticas de la clase 2.

## Tiempo y ventanas

**8.** El event time es cuando ocurrió la compra y el processing time es cuando llega al sistema. late_ok ocurrió a las 12:03 pero recién llegó en la llegada 2.

**9.** 11:54, 11:58, 12:16, 12:17 y 12:35. Se calcula como el máximo event time visto menos 10 minutos.

**10.** En la llegada 3. Antes no porque en append la ventana se emite cuando el watermark pasa su fin, y hasta la llegada 2 el watermark seguía antes de las 12:05.

**11.** late_bad se descartó y no sumó en ninguna ventana. late_ok se aceptó porque cuando llegó el watermark estaba en 11:54, en cambio late_bad llegó con el watermark en 12:16 y ya era muy viejo.

**12.** Tumbling son ventanas fijas que no se solapan, sirve para ver ventas cada 5 minutos. Hopping son ventanas que se solapan, entonces una compra puede caer en dos, sirve para un promedio móvil. Session agrupa por cliente y se cierra cuando deja de comprar, sirve para medir cada visita de un cliente.

**13.** En update se emitió 2 veces: 40 compras y 6000 en la llegada 1, y 60 compras y 8400 en la llegada 2. En append una sola vez, en la llegada 3, con 60 compras y 8400. Update da resultados antes pero hay que tolerar recibir la misma ventana varias veces, append es más simple pero tarda más.

**14.** Se gana latencia porque las ventanas cerrarían antes. Se pierde completitud, ya que se descartarían más eventos tardíos.

## Garantías y fallas

**15.** Sin deduplicar 220 y deduplicando 200. El productor puede mandar el mismo evento dos veces si no recibe confirmación y reintenta, como pasó con e002.

**16.** Con el mismo checkpoint no se escribió nada nuevo y quedaron 200, eso es exactly-once. Al perderlo se procesó todo de nuevo y quedaron 400, eso es at-least-once.

**17.** Porque MERGE solo inserta el evento si su event_id no está en el destino, entonces quedó en 200. Que sea idempotente significa que aplicarla dos veces da lo mismo que aplicarla una.

## CDC

**18.** 13 eventos: 6 insert, 3 update_preimage, 3 update_postimage y 1 delete. Un UPDATE genera dos porque guarda la fila como estaba antes y como quedó después.

**19.** Sí, quedó igual a la original. El log tiene el historial que la tabla no tiene, como que el cliente 2 estuvo en UY y CL antes de BR, o que existió el cliente 3.
