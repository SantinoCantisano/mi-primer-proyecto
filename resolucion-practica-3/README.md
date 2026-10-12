# Entrega práctica 3 — Bases de datos NoSQL

- **Nombre:** Santino Cantisano
- **student_id:** santino_cantisano
- **Escala:** small

## CAP y PACELC

**1.** En modo CP la réplica C retorna error tanto en read como en write porque queda aislada y sin quórum. En modo AP responde igual, pero con datos desactualizados (devuelve 100 cuando ya se había escrito 80). En CP se pierde disponibilidad, y en AP, consistencia.

**2.** 50. Se perdió la escritura de 80 que se hizo en A, ya que al reparar ganó la de C por ser la más nueva (last-write-wins).

**3.** 9.6 y 55.6 respectivamente. Con W=3 hay que esperar también a la réplica de la otra región, por eso la latencia sube tanto. A cambio se gana mayor consistencia, ya que la escritura queda confirmada en todas las réplicas.

**4.** De lado ABC ya que es el lado con mayoría de réplicas (3 de 5), entonces tiene quórum.

## Clave-valor

**5.** Es 2,002,726 veces más rápido. Redis guarda los datos en memoria para poder accederlos más rápidamente, ya que leer de RAM es mucho más rápido que ir a disco o lanzar una consulta analítica como hace Spark.

**6.** Porque en Redis el valor no está disponible facilmente en una columna sino que está dentro del string de texto. Tiene la ventaja de que es muy rápido, pero no se puede consultar por los campos del valor sin leerlos todos o armar índices a mano.

**7.** 79.6 y 21.2. Conviene el segundo ya que mueve muchos menos datos entre los nodos al agregar uno nuevo (solo las claves del tramo del anillo que ocupa), y con los nodos virtuales la carga queda distribuida de manera equilibrada.

## Documental

**8.** La relacional devolvió 11 filas y la documental, 1 solo documento. Los datos embebidos dentro del documento corresponden a la misma información disponible en la relacional pero con formato no columnar: las transacciones del cliente, y dentro de cada una una copia del producto (id, categoría y precio).

**9.** Spark armó un esquema con la unión de todos los campos y completó con null los que faltaban en cada documento. Tiene esquema flexible ya que no se rompe en caso de que los datos vengan con formatos distintos a los habituales, no hay campos obligatorios fijos y el esquema se infiere al leer.

**10.** Pesa 5.2kb. Si creciese sin límite realentizaría mucho el proceso de lectura, y además MongoDB tiene un límite de 16 MB por documento (en este caso unas 74,599 transacciones).

**11.** Resuelve bien consultas de acceso por entidad, como traer un cliente con todo su historial en una sola lectura. Le cuesta más la analítica transversal (por ejemplo monto por categoría) ya que demanda desarmar los arrays y pasar a formato de filas.

## Grafos

**12.** El grafo tiene 5000 nodos Customer, 2361 nodos Device y 5400 aristas USES.

**13.** 3 y 5 respectivamente. Recorre las relaciones más eficientemente porque no realiza joins, cada nodo tiene punteros directos a sus vecinos.

**14.** Tardó 14 iteraciones en converger, en la última ya no cambió ningún nodo de componente. El componente más grande tiene 23 nodos, que sería una posible red de cuentas conectadas por dispositivos.

**15.** 74.4%. Es difícil de distribuir porque muchas aristas quedan con sus extremos en máquinas distintas, y para recorrerlas fuerza saltos en la red, aumentando la latencia. Agrupando por componente se cortan muchas menos, pero no siempre se pueden encontrar particiones así de limpias.

## Vectorial

**16.** Representa el comportamiento de compra del cliente en 10 números: cuánto gasta en cada categoría, con qué canal paga, el ticket promedio y la proporción de compras grandes. La similitud coseno mide qué tan parecidos son dos vectores según su dirección, si da 1 los clientes se comportan igual.

**17.** 31.6% de marcados entre los vecinos de marcados, y la tasa base es 14.8%. Sirve para encontrar clientes que se comportan parecido a uno sospechoso y revisarlos, aunque como clasificador solo no le gana a predecir siempre no marcado (85.9% vs 86.3%).

**18.** Recall de 0.872 escaneando el 5.99% de los vectores. Se gana velocidad ya que no compara contra todos, pero se pierde precisión porque algunos vecinos reales no los encuentra.

## Columnar

**19.** CSV 50 MB, JSON 121.1 MB y Parquet 5.3 MB. Leyó solo la columna amount. Con los datos ordenados se pueden saltear 7 de 8 archivos.

**20.** Es bueno para analítica porque lee solo las columnas que necesita, comprime mejor y puede saltear archivos con el min/max. No conviene para modificar una fila porque los datos de esa fila están separados en cada columna, entonces hay que reescribir el archivo entero.
