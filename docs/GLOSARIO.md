# Glosario

Conceptos que han ido apareciendo en el proyecto, explicados con ejemplos de
este mismo código. El orden es temático, no alfabético: leídos de arriba abajo
se van apoyando unos en otros.

---

## Arquitectura

### Medallion (bronze / silver / gold)

Tres capas con responsabilidades distintas. La idea de fondo: **cada capa puede
reconstruirse desde la anterior**, así que un error de negocio solo obliga a
reprocesar, nunca a volver a molestar al sistema origen.

| Capa | Qué es | Regla |
|---|---|---|
| **Bronze** | Copia fiel del origen | No se limpia nada. Append-only. |
| **Silver** | Datos limpios y usables | Una fila por entidad, validada. |
| **Gold** | Datos listos para consumir | Modelados para responder preguntas de negocio. |

La regla de Bronze parece una limitación y es justo lo contrario: si Bronze
filtrara filas malas, el día que cambies el criterio ya no podrías recuperarlas.

### Grano

La respuesta a **"¿qué representa exactamente una fila de esta tabla?"**.

Es la primera decisión de una tabla de hechos y la más difícil de corregir
después. En este proyecto, `fct_order_items` tiene grano de **línea de pedido**:
una fila = un producto dentro de un pedido.

Se eligió el grano más atómico disponible porque **desde el grano fino se puede
agregar a cualquier nivel, pero al revés no**. Con grano de pedido no podríamos
calcular unidades vendidas por categoría, que es precisamente lo que pide
`agg_daily_sales`.

Consecuencia directa: `fct_order_items` **no lleva** `total_amount` del pedido.
Repetir el total en cada línea lo duplicaría al sumar. El total de un pedido se
obtiene sumando sus líneas.

### Idempotencia

Que ejecutar algo N veces deje el mismo resultado que ejecutarlo una vez.

Es lo que permite reintentar sin miedo, y sin ella un pipeline es una bomba de
relojería: cada reejecución infla los datos y nadie se entera hasta que los
números de negocio dejan de cuadrar.

**Cuidado con cómo se verifica.** Que el job termine sin error no la demuestra.
En este proyecto pasó: el job de siembra imprimía "349.009 filas cargadas" en
las dos ejecuciones, pero eso solo contaba lo que el job *escribía*. Si el
`TRUNCATE` hubiera fallado, el log habría sido idéntico y la tabla tendría el
doble. La comprobación válida es **leer el estado final del sistema**, que es lo
que hacen ahora las verificaciones con Athena.

### Watermark

La marca de "hasta dónde leí la última vez". Se guarda una por tabla y la
siguiente ejecución solo pide lo posterior.

En este proyecto viven en SSM Parameter Store (`/practica/dev/watermark/orders`)
y guardan el último `updated_at` procesado.

Dos detalles que evitan perder filas en silencio:

- El watermark avanza hasta el último `updated_at` **leído de verdad**, no hasta
  "ahora". Con la hora actual, cualquier fila confirmada en el origen mientras
  el job leía quedaría fuera para siempre.
- Se escribe **solo al terminar bien**. Si el job falla después de escribir en
  S3, la siguiente ejecución repite filas. Repetir es inofensivo (Silver
  deduplica); perder no.

### Backfill

Reprocesar datos del pasado, normalmente porque cambió una regla o se arregló un
bug. Aquí se hace retrasando el watermark:

```bash
aws ssm put-parameter --overwrite --type String \
  --name /practica/dev/watermark/orders --value 2026-01-01T00:00:00+00:00
make bronze TABLES=orders
```

### Linaje

El rastro de dónde viene cada fila. Bronze añade `_ingested_at`,
`_source_system` y `_batch_id` (el id del run de Glue).

Sirve para que, cuando dentro de seis meses alguien pregunte "¿de dónde sale
este dato?", la respuesta sea una consulta y no una arqueología.

---

## Calidad e integridad

### Cuarentena

Apartar las filas que fallan la validación en lugar de descartarlas, guardando
**el motivo** del rechazo.

Descartar en silencio es la forma más rápida de perder la confianza en un data
lake: los números no cuadran y nadie sabe por qué. Aquí van a
`silver/_quarantine/<tabla>/` con `_quality_errors`, un array con *todos* sus
problemas — si un pedido tiene el importe negativo y el cliente huérfano, te
enteras de las dos cosas a la vez en lugar de arreglar una y descubrir la otra
mañana.

### Clave foránea huérfana

Una clave que apunta a algo que no está. Pero **hay dos casos muy distintos**, y
confundirlos costó un rediseño en este proyecto:

- Apunta a algo que **nunca existió** en el origen → dato malo de verdad.
- Apunta a algo que **existió pero no sobrevivió a la limpieza** → el dato es
  correcto, el problema es del padre.

### Efecto cascada (amplificación de la cuarentena)

Lo que pasa cuando se trata el segundo caso como el primero. Medido con los
datos de prueba:

```
   628 clientes a cuarentena (email nulo, país mal formado)
         ↓ dejaban huérfanos
 3.253 pedidos
         ↓ dejaban huérfanas
19.303 líneas de pedido
```

Un 1% de huérfanos reales se convertía en un **10%** de cuarentena: casi 7× de
amplificación, y la tasa dejaba de significar nada.

La solución fue validar contra **el universo de claves vistas en el origen**
(todo el lote, incluidas las filas que van a cuarentena) en lugar de contra las
supervivientes. Ver `src/common/quality.py`.

### Referencialmente cerrada

Una capa es referencialmente cerrada si **ninguna fila apunta a algo que no esté
en esa misma capa**.

**Silver, en este proyecto, no lo es — a propósito.** Puede haber un pedido cuyo
`customer_id` no exista en `silver.customers` porque ese cliente está en
cuarentena.

Es un compromiso consciente:

| | Cerrada | No cerrada (lo que hacemos) |
|---|---|---|
| Consistencia interna | garantizada | hay que resolverla aguas abajo |
| Efecto cascada | sí, y amplifica | no |
| Los ingresos cuadran | no, se pierden ventas | sí |

Quien hereda el problema es Gold, y lo resuelve con el miembro desconocido.

---

## Modelado dimensional

### Modelo estrella

Una **tabla de hechos** en el centro rodeada de **dimensiones**, cada una a un
solo join de distancia. De ahí el nombre.

Lo natural sería normalizar (cliente → ciudad → provincia → país, cada uno en su
tabla). El modelo estrella deliberadamente **no** lo hace, y por dos razones:

1. **Menos joins.** Una consulta analítica típica cruza hechos con 3-5
   dimensiones. Con un esquema normalizado serían 15 joins y un plan de
   ejecución imprevisible.
2. **Se entiende.** Alguien de negocio puede leer el modelo y saber qué
   preguntar. Un esquema normalizado exige conocer el modelo relacional entero.

Se paga con redundancia (el país se repite en cada fila de `dim_customer`), y
compensa: el almacenamiento es barato y las dimensiones son pequeñas.

### Tabla de hechos vs dimensión

- **Hecho**: algo que **ocurrió**, con medidas numéricas. Muchas filas, crece sin
  parar. Aquí: `fct_order_items`.
- **Dimensión**: el **contexto** de ese algo — quién, qué, cuándo, dónde. Pocas
  filas, muchas columnas descriptivas. Aquí: `dim_customer`, `dim_product`,
  `dim_date`.

Regla práctica: si es un número que quieres sumar, es una medida y va al hecho.
Si es un atributo por el que quieres agrupar o filtrar, va a una dimensión.

### Medida aditiva

Una medida que se puede sumar por cualquier dimensión y sigue teniendo sentido.
`quantity` y `line_amount` lo son.

`unit_price` **no** lo es: sumar precios unitarios no significa nada. Se guarda
porque sirve para auditar, pero no se agrega.

Un total de pedido repetido en cada línea sería peor: parecería aditivo y no lo
es. Por eso no está.

### Clave subrogada vs clave natural

- **Natural** (o de negocio): la del origen, `customer_id = 4821`.
- **Subrogada**: la que inventa el almacén, `customer_key`.

Con SCD tipo 2 la subrogada es **imprescindible**, no un capricho: un mismo
`customer_id` tiene varias filas en la dimensión, una por versión, y el hecho
tiene que apuntar a una concreta.

Aquí se calculan con un **hash determinista** (`xxhash64`) de la clave de negocio
más `valid_from`, en vez de con un contador. Con un contador
(`monotonically_increasing_id`) cada reconstrucción asignaría claves distintas y
los hechos ya escritos apuntarían a la fila equivocada.

### Dimensión degenerada

Un identificador que vive en el hecho porque no tiene atributos que colgar de
él. `order_id` en `fct_order_items` es uno: sirve para agrupar líneas del mismo
pedido y para rastrear, pero una `dim_order` que solo tuviera el id no aportaría
nada.

### Miembro desconocido

La fila `-1` de cada dimensión, a la que apuntan los hechos huérfanos.

Existe porque descartar esos hechos **falsearía los ingresos**: son ventas
reales. Mandarlos al miembro desconocido los conserva y además deja el problema
visible — puedes preguntar "¿cuánto facturamos a clientes desconocidos?" y
perseguirlo.

La clave es negativa a propósito: nunca colisiona con un hash y se ve a simple
vista.

### SCD tipo 1 y tipo 2

*Slowly Changing Dimension*: cómo se trata un atributo de dimensión que cambia.

- **Tipo 1**: se sobrescribe. No hay historia. Aquí, `dim_product`.
- **Tipo 2**: se cierra la versión anterior y se abre una nueva, con
  `valid_from`, `valid_to` e `is_current`. Aquí, `dim_customer`.

Elegir uno u otro es una decisión de negocio, no técnica. `dim_product` es tipo 1
porque el precio al que se vendió de verdad ya está en la línea de pedido, que
es donde debe estar; la dimensión solo aporta nombre y categoría.

`dim_customer` es tipo 2 porque quieres poder decir *"cuánto vendimos a clientes
gold en marzo"* con el segmento que tenían **entonces**, no con el de hoy.

No todos los cambios merecen versión: aquí solo la abren `segment`,
`country_code` y `marketing_opt_in`. Corregir una errata en el nombre no cambia
ningún análisis.

**Detalle que ha causado bugs silenciosos en medio mundo**: comparar atributos
con `!=` en vez de `<=>` (igual null-safe). En SQL, `NULL != 'gold'` es NULL —
ni verdadero ni falso — así que un cambio desde NULL pasa desapercibido y la
historia se pierde sin que nada falle.

### Join point-in-time (*as-of join*)

Unir un hecho con la versión de la dimensión **vigente en la fecha del hecho**:

```sql
ON  f.customer_id = d.customer_id
AND f.order_date >= d.valid_from
AND f.order_date <  d.valid_to
```

Es para lo que existe el SCD tipo 2. Dos formas de equivocarse:

- Join solo por clave natural → devuelve una fila por versión y **multiplica los
  hechos**. Los ingresos se multiplican y nada falla.
- Join contra `is_current` → da el estado de **hoy**, que es justo lo que no
  quieres al analizar el pasado.

`valid_to` usa una fecha centinela (`9999-12-31`) en vez de NULL para que la
condición se escriba sin un `OR valid_to IS NULL` que además impide aprovechar
particiones.

### Snowflake

Variante del modelo estrella en la que las dimensiones se normalizan en varias
tablas (`dim_product` → `dim_category` → `dim_department`).

Ahorra algo de espacio y añade joins y complejidad. Rara vez compensa: las
dimensiones son pequeñas.

---

## Formatos y almacenamiento

### Table format (Iceberg, Delta, Hudi)

Una capa por encima de los ficheros que aporta lo que Parquet suelto no tiene:
transacciones, `UPDATE`/`DELETE`, evolución de esquema y viaje en el tiempo.

En este proyecto: **Bronze es Parquet plano, Silver y Gold son Iceberg**. Bronze
no necesita nada de eso porque es append-only e inmutable, y pagar el coste de
un table format para escribir y no volver a tocar es como usar una base de datos
transaccional para guardar logs.

Un efecto lateral muy útil: una tabla Iceberg **se registra sola en el Glue Data
Catalog**, así que Silver y Gold son consultables desde Athena sin declarar nada.

### Upsert (`MERGE INTO`)

Insertar si no existe, actualizar si existe, en una sola operación atómica:

```sql
MERGE INTO tabla t USING lote s ON t.clave = s.clave
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Es lo que hace idempotente a Silver.

### Partición oculta

En Hive, particionar por mes obliga a crear una columna `mes` y a que **quien
consulta se acuerde de filtrar por ella**; si filtra solo por `order_date`, lee
la tabla entera y nadie se entera salvo por la factura.

Iceberg guarda la transformación (`months(order_date)`) como metadato: filtras
por `order_date` y resuelve solo qué particiones leer.

### Time travel

Consultar el estado de una tabla en un momento anterior, usando los *snapshots*
que Iceberg escribe en cada commit:

```sql
SELECT * FROM practica_dev_silver.customers FOR SYSTEM_TIME AS OF '2026-08-05 10:00:00';
```

Convierte una auditoría de tres días en una consulta.

### Problema de los ficheros pequeños

Cada escritura crea ficheros nuevos. Con ejecuciones frecuentes acabas con miles
de ficheros diminutos, y leer 10.000 ficheros de 1 MB es mucho más lento que
leer 100 de 100 MB — el coste está en abrirlos, no en los bytes.

Iceberg lo resuelve con compactación (`rewrite_data_files`).

### Evolución de esquema

Añadir, renombrar o cambiar el tipo de una columna sin reescribir la tabla ni
romper a quien la consulta. Iceberg lo soporta porque sigue las columnas por un
id interno, no por su posición en el fichero.

---

## Red y operación en AWS

### VPC endpoint

Una puerta desde tu red privada a un servicio de AWS, sin pasar por internet.

En este proyecto la VPC **no tiene salida a internet**, así que un servicio sin
endpoint sencillamente **no existe** para los jobs. Costó un fallo real: el job
de Bronze moría con `ConnectTimeoutError` contra SSM tras dos minutos, sin decir
en ningún momento que faltara un endpoint.

Cuidado con la aritmética: los endpoints de **interfaz** se pagan por endpoint
**y por AZ**. El **gateway** de S3 es gratis.

### Glue Connection

Pese al nombre, no es "una conexión a una base de datos": es lo que hace que un
job de Glue **se ejecute dentro de tu VPC**, creando ENIs en tu subred.

Solo la necesitan los jobs que hablan con el RDS. Silver y Gold corren sin ella:
sin ENIs, sin depender de endpoints y arrancando antes.
