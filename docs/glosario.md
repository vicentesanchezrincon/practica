<div class="portada" markdown="1">

# Glosario

<p class="sub">Términos, siglas, arquitecturas y modos de fallo</p>

<p class="meta">
Manual de referencia del proyecto <b>practica</b><br>
Pipeline medallion · AWS Glue · PySpark · Apache Iceberg · Step Functions
</p>

</div>

<div class="toc" markdown="1">

# Índice

[TOC]

</div>

# Cómo usar este glosario

Este documento explica **conceptos**, no este proyecto. Lo que aquí se define
vale igual en cualquier plataforma de datos: qué es un modelo estrella, por qué
un `NULL` rompe una comparación, qué hace realmente una Glue Connection. La
historia concreta de *practica* —qué se decidió, qué se rompió y qué se aprendió
en cada fase— está en el otro documento, `documentacion-practica.pdf`.

La frontera entre los dos es esta:

| Documento del proyecto | Este glosario |
|---|---|
| «Se eligió Iceberg y no Delta porque…» | Qué es un *table format* y en qué se diferencian Iceberg, Delta y Hudi |
| «El stack de red quedó en `DELETE_FAILED` el 4 de agosto» | Por qué una ENI huérfana impide borrar una VPC |
| «La cuarentena de `order_items` subió del 2% al 9,95%» | Qué es el efecto cascada en la validación referencial |

**El orden es temático, no alfabético.** Leídos de arriba abajo, los términos se
van apoyando unos en otros: no se puede entender una clave subrogada sin haber
entendido antes qué es una dimensión. Para buscar un término suelto, el índice.

Cuando un concepto se ilustra con un ejemplo del proyecto, aparece en un aviso
como este:

!!! nota "Visto en el proyecto"
    El ejemplo concreto, con su cifra o su fichero. El concepto se entiende sin
    leerlo; el ejemplo solo lo aterriza.

---

# 1. Siglas

Las siglas del sector están tan interiorizadas que casi nunca se expanden, y eso
convierte cualquier documentación en un jeroglífico para quien empieza. Aquí
están todas las que aparecen en el proyecto, con lo que significa **cada letra**.

## 1.1 Siglas de AWS

AWS
:   *Amazon Web Services*. La plataforma de servicios en la nube de Amazon.

S3
:   *Simple Storage Service*. Almacenamiento de objetos: guardas ficheros
    identificados por una clave, no en un sistema de ficheros con directorios
    de verdad. Lo que parecen carpetas son prefijos del nombre.

RDS
:   *Relational Database Service*. Bases de datos relacionales gestionadas:
    AWS se ocupa de instalar, parchear y hacer copias de seguridad.

VPC
:   *Virtual Private Cloud*. Tu red privada dentro de AWS, aislada de las de
    los demás clientes.

AZ
:   *Availability Zone*. Zona de disponibilidad: uno o varios centros de datos
    independientes dentro de una región. Repartir entre AZ es lo que da
    tolerancia a que se caiga un edificio.

EC2
:   *Elastic Compute Cloud*. Máquinas virtuales. Muchos servicios gestionados
    (Glue incluido) corren sobre EC2 por debajo sin que se vea.

ENI
:   *Elastic Network Interface*. Una tarjeta de red virtual. Cuando un servicio
    gestionado necesita meterse en tu VPC, lo que hace es crear ENIs en tus
    subredes.

SG
:   *Security Group*. Cortafuegos a nivel de interfaz de red. Con estado: si
    permites la conexión de salida, la respuesta vuelve sola.

IGW
:   *Internet Gateway*. La puerta de una VPC a internet. Una VPC sin IGW no
    puede salir aunque tenga rutas.

NAT
:   *Network Address Translation*. Un NAT Gateway permite que las subredes
    privadas salgan a internet sin ser alcanzables desde fuera. Cuesta fijo al
    mes, y es el error de coste número uno en proyectos de datos.

IAM
:   *Identity and Access Management*. Quién puede hacer qué sobre qué recurso.

ARN
:   *Amazon Resource Name*. El identificador único de cualquier recurso:
    `arn:aws:s3:::mi-bucket/clave`.

STS
:   *Security Token Service*. El servicio que emite credenciales temporales.
    Es quien responde cuando alguien «asume un rol».

KMS
:   *Key Management Service*. Gestión de claves de cifrado.

SNS
:   *Simple Notification Service*. Publicar mensajes en un *topic* al que se
    suscriben correos, colas o funciones.

SSM
:   *Systems Manager*. Un cajón de herramientas de operación. Aquí interesa
    **Parameter Store**, que guarda valores de configuración con versionado.

CDK
:   *Cloud Development Kit*. Escribir infraestructura en un lenguaje de
    programación de verdad, que se compila a CloudFormation.

CFN
:   *CloudFormation*. El motor de infraestructura como código de AWS: recibe
    una plantilla y se encarga de crear, modificar y borrar lo que haga falta.

DPU
:   *Data Processing Unit*. La unidad de facturación de Glue: 4 vCPU y 16 GB.
    Se paga por DPU y minuto.

ETL
:   *Extract, Transform, Load*. Extraer, transformar y cargar. La variante
    **ELT** carga primero y transforma después, ya dentro del almacén; es lo
    que hace una arquitectura medallion.

CLI
:   *Command Line Interface*. La herramienta de línea de comandos.

OIDC
:   *OpenID Connect*. Protocolo de identidad sobre OAuth 2.0. Permite que un
    sistema externo demuestre quién es sin guardar ninguna clave. Ver §10.

ECR
:   *Elastic Container Registry*. El registro de imágenes de contenedor de AWS.
    `public.ecr.aws` es su parte pública.

## 1.2 Siglas de datos y bases de datos

SQL
:   *Structured Query Language*. El lenguaje de consulta de las bases
    relacionales.

DDL
:   *Data Definition Language*. La parte de SQL que define estructuras:
    `CREATE TABLE`, `ALTER TABLE`.

DML
:   *Data Manipulation Language*. La que mueve datos: `INSERT`, `UPDATE`,
    `DELETE`, `SELECT`.

JDBC
:   *Java Database Connectivity*. El estándar de Java para hablar con bases de
    datos. Spark corre sobre la JVM, así que lee de Postgres por JDBC.

PK
:   *Primary Key*. Clave primaria: identifica una fila de forma única.

FK
:   *Foreign Key*. Clave foránea: apunta a la clave primaria de otra tabla.

CDC
:   *Change Data Capture*. Capturar solo lo que ha cambiado desde la última vez,
    en lugar de releerlo todo. Ver **watermark** (§3).

SCD
:   *Slowly Changing Dimension*. Dimensión que cambia despacio. Ver §4.

ACID
:   *Atomicity, Consistency, Isolation, Durability*. Las cuatro garantías de una
    transacción: o pasa entera o no pasa, deja los datos coherentes, no se
    entera de lo que hacen otras, y una vez confirmada no se pierde.

OLTP
:   *OnLine Transaction Processing*. Sistemas de operación diaria: muchas
    escrituras pequeñas, lecturas por clave. Un Postgres de producción.

OLAP
:   *OnLine Analytical Processing*. Sistemas de análisis: pocas consultas,
    enormes, que recorren millones de filas para agregarlas.

URI / URL
:   *Uniform Resource Identifier / Locator*. La forma de nombrar un recurso.
    `s3://bucket/clave` es un URI.

CSV
:   *Comma-Separated Values*. Texto plano separado por comas. Sin tipos, sin
    esquema y sin forma de leer solo una columna.

TZ
:   *Time Zone*. Zona horaria. Guardar fechas sin ella es una fuente inagotable
    de errores de un día.

SKU
:   *Stock Keeping Unit*. El código con el que un comercio identifica un
    producto concreto.

## 1.3 Siglas de desarrollo y sistema

JVM
:   *Java Virtual Machine*. La máquina virtual sobre la que corre Spark. Por eso
    un contenedor de Spark pesa gigabytes.

JAR
:   *Java ARchive*. Un fichero con código Java empaquetado. Las librerías de
    Iceberg y los drivers JDBC son JAR.

uid / gid
:   *user id / group id*. Los números con los que el sistema operativo identifica
    a un usuario y a un grupo. Que no coincidan entre el host y un contenedor es
    la causa más común de «permission denied» al montar un volumen.

API
:   *Application Programming Interface*. El contrato por el que dos programas se
    hablan.

SDK
:   *Software Development Kit*. La librería que envuelve una API para un
    lenguaje concreto (`boto3` es el SDK de AWS para Python).

YAML / JSON / TOML
:   Tres formatos de configuración legibles. YAML usa indentación (los workflows
    de GitHub Actions), JSON es el de las APIs y las plantillas de
    CloudFormation, TOML el de `pyproject.toml`.

PR
:   *Pull Request*. La propuesta de fusionar una rama en otra, con su revisión
    y sus comprobaciones automáticas.

CI / CD
:   *Continuous Integration / Continuous Delivery (o Deployment)*. Integrar y
    validar cada cambio automáticamente, y desplegarlo automáticamente. Ver §10.

TDD
:   *Test-Driven Development*. Escribir la prueba antes que el código.

SSL / TLS
:   *Secure Sockets Layer / Transport Layer Security*. El cifrado del tráfico.
    SSL es el nombre antiguo; lo que se usa hoy es TLS.

DNS
:   *Domain Name System*. Traduce nombres a direcciones IP.

IP
:   *Internet Protocol*. La dirección de una máquina en una red.

CIDR
:   *Classless Inter-Domain Routing*. La notación `10.20.0.0/16` para describir
    un rango de direcciones. Ver §8.

TCP
:   *Transmission Control Protocol*. El protocolo de transporte fiable y
    ordenado sobre el que van HTTP, JDBC y casi todo.

---

## 1.4 Siglas de series temporales y ficheros

TSDB
:   *Time Series Database*. Base de datos especializada en series temporales:
    muchísimas escrituras en orden cronológico, casi ninguna actualización, y
    consultas que siempre acotan un rango de tiempo. Es la familia de base de
    datos que define al sector energético.

PSP
:   *Payment Service Provider*. La pasarela de pago. Liquida a comercio con uno
    o varios días de retraso, descontando su comisión, y lo comunica en un
    fichero diario.

DST
:   *Daylight Saving Time*. El horario de verano. Dos veces al año la hora local
    deja de ser una función biyectiva del instante. Ver §11.

UUID
:   *Universally Unique Identifier*. Identificador de 128 bits que se genera sin
    coordinación central. Lo puede generar el cliente, que es su gracia en un
    flujo de eventos, y su maldición para paralelizar una lectura por rango.

BRIN
:   *Block Range Index*. Índice que guarda el rango de valores de cada bloque de
    disco en vez de una entrada por fila. Ocupa órdenes de magnitud menos que un
    btree y funciona muy bien cuando los datos llegan ya ordenados en el tiempo.

CDC
:   *Change Data Capture*. Capturar los cambios de una base de datos en vez de
    releerla entera. Por watermark es la versión pobre y perfectamente válida;
    la versión completa lee el log de transacciones.

# 2. Nombres propios y herramientas

Apache Spark
:   Motor de procesamiento distribuido. Reparte un cálculo entre muchas máquinas
    y lo coordina. Ver §7 para su vocabulario.

Apache Parquet
:   Formato de fichero **columnar** y comprimido. Ver §6.

Apache Iceberg
:   *Table format* abierto. Añade transacciones, `UPDATE`/`DELETE` y viaje en el
    tiempo sobre ficheros Parquet. Ver §6.

Apache Hive
:   El almacén de datos original sobre Hadoop. Su legado más persistente es el
    *metastore* (el catálogo de tablas) y su forma de particionar por
    directorios, que Iceberg viene a superar.

AWS Glue
:   Servicio de ETL gestionado. Ejecuta trabajos de Spark sin que tengas que
    montar un clúster, e incluye el **Glue Data Catalog**, el registro de qué
    tablas existen y dónde están sus ficheros.

Amazon Athena
:   Motor de consulta SQL sobre ficheros de S3, sin servidor. Se paga por
    terabyte escaneado, lo que convierte el particionado en una decisión de
    factura.

AWS Step Functions
:   Orquestador de flujos de trabajo. Se define una máquina de estados y el
    servicio se encarga de ejecutar cada paso, esperar, reintentar y capturar
    errores. Ver §9.

Amazon EventBridge
:   Bus de eventos. Sirve, entre otras cosas, para lanzar algo con un horario
    (`cron`).

AWS Secrets Manager
:   Almacén de secretos con rotación. La contraseña de una base de datos vive
    aquí y no en el código.

AWS PrivateLink
:   La tecnología detrás de los VPC endpoints de interfaz: expone un servicio de
    AWS como una interfaz de red dentro de tu VPC.

PostgreSQL
:   Base de datos relacional de código abierto. Aquí hace de sistema origen.

Docker
:   Contenedores: empaquetar una aplicación con todo su entorno para que se
    ejecute igual en cualquier sitio.

Terraform
:   La alternativa más extendida al CDK para infraestructura como código,
    con su propio lenguaje declarativo (HCL) en vez de un lenguaje de
    programación general.

Faker
:   Librería que genera datos falsos con pinta de reales (nombres, correos,
    direcciones).

pytest
:   El marco de pruebas estándar de Python.

ruff
:   Linter y formateador de Python, escrito en Rust. Sustituye a la vez a flake8,
    isort y black, y es mucho más rápido.

pre-commit
:   Herramienta que ejecuta comprobaciones antes de cada commit, para que los
    errores triviales no lleguen ni al repositorio.

---

TimescaleDB
:   Extensión de PostgreSQL que convierte una tabla en *hypertable*: por dentro
    se trocea en fragmentos por rango de tiempo, y por fuera se consulta como
    una tabla normal. **RDS no la ofrece** en ninguna versión.

OSIsoft PI System
:   El *historian* clásico de la industria y de la energía. Propietario, caro y
    omnipresente en plantas de generación.

InfluxDB
:   Base de datos de series temporales de código abierto, muy usada en
    monitorización y en IoT.

Amazon Timestream
:   La TSDB gestionada de AWS. Sin servidores que administrar y con facturación
    por escritura y consulta.

# 3. Arquitecturas y patrones

## Data lake, data warehouse y lakehouse

Tres cosas que se confunden constantemente:

- **Data warehouse**: datos ya estructurados y modelados, dentro de un sistema
  propietario que controla el almacenamiento y el motor de consulta. Rápido y
  fiable; caro, y solo entra lo que decidiste modelar de antemano.
- **Data lake**: ficheros en un almacenamiento barato, en cualquier formato,
  sin esquema obligatorio. Barato y flexible; degenera en un pantano en cuanto
  nadie sabe qué hay ni si es fiable.
- **Lakehouse**: ficheros en almacenamiento barato **más** una capa de metadatos
  que aporta transacciones, esquema y tiempo. Es lo que hacen los *table
  formats*, y es la arquitectura que usa este proyecto.

## Arquitectura medallion

Tres capas con responsabilidades distintas. La idea de fondo: **cada capa puede
reconstruirse desde la anterior**, así que un error de negocio obliga a
reprocesar, nunca a volver a molestar al sistema origen.

| Capa | Qué es | Regla |
|---|---|---|
| **Bronze** | Copia fiel del origen | No se limpia nada. Append-only. |
| **Silver** | Datos limpios y usables | Una fila por entidad, validada. |
| **Gold** | Datos listos para consumir | Modelados para responder preguntas de negocio. |

La regla de Bronze parece una limitación y es justo lo contrario: si Bronze
filtrara las filas malas, el día que cambies el criterio ya no podrías
recuperarlas. El sistema origen no guarda historia de lo que borró.

## Grano

La respuesta a **«¿qué representa exactamente una fila de esta tabla?»**.

Es la primera decisión de una tabla de hechos y la más cara de corregir después.
La regla es elegir **el grano más atómico disponible**, porque desde el grano
fino se puede agregar a cualquier nivel, y al revés no.

!!! nota "Visto en el proyecto"
    `fct_order_items` tiene grano de **línea de pedido**: una fila = un producto
    dentro de un pedido. Con grano de pedido no se podrían calcular unidades por
    categoría. Consecuencia directa: la tabla **no lleva** el total del pedido,
    porque repetirlo en cada línea lo duplicaría al sumar.

## Idempotencia

Que ejecutar algo N veces deje el mismo resultado que ejecutarlo una vez.

Es lo que permite reintentar sin miedo. Sin ella un pipeline es una bomba de
relojería: cada reejecución infla los datos y nadie se entera hasta que los
números de negocio dejan de cuadrar, meses después.

**Cuidado con cómo se verifica.** Que el trabajo termine sin error no la
demuestra. Un job que imprime «349.009 filas cargadas» en las dos ejecuciones
solo está contando lo que **él escribió**; si el `TRUNCATE` previo hubiera
fallado, el mensaje sería idéntico y la tabla tendría el doble. La comprobación
válida es **leer el estado final del sistema**, no el registro del proceso.

## Watermark

La marca de «hasta dónde leí la última vez». Se guarda una por tabla y la
siguiente ejecución solo pide lo posterior. Es la forma más simple de CDC.

Dos detalles que evitan perder filas en silencio:

- El watermark avanza hasta el último valor **leído de verdad**, no hasta
  «ahora». Usando la hora actual, cualquier fila confirmada en el origen
  *mientras* el proceso leía quedaría fuera para siempre.
- Se escribe **solo al terminar bien**. Si el proceso falla después de escribir
  los datos, la siguiente ejecución repite filas. Repetir es inofensivo si la
  capa siguiente deduplica; perder no tiene arreglo.

## Backfill

Reprocesar datos del pasado, normalmente porque cambió una regla o se arregló un
bug. Con una arquitectura medallion se hace desde Bronze, sin tocar el origen; y
con watermarks, retrasando el watermark al punto desde el que se quiere releer.

## Linaje

El rastro de dónde viene cada fila: de qué sistema, en qué ejecución, cuándo.
Sirve para que la pregunta «¿de dónde sale este dato?» se responda con una
consulta y no con una excavación.

## Walking skeleton

Construir primero un recorrido **fino pero completo** de punta a punta —una
tabla, una transformación trivial, un despliegue real— y engordarlo después.

Lo contrario es construir cada capa entera antes de conectar nada, que es más
cómodo y esconde los problemas de integración hasta el final, cuando ya se han
tomado decisiones difíciles de deshacer. Dejar la orquestación para el último
paso es el caso más habitual de esto.

## Infraestructura como código (IaC)

Describir la infraestructura en ficheros versionados en vez de crearla a mano
en una consola web. Lo que se gana no es velocidad, es **saber qué hay**: la
consola no tiene historial, ni revisión, ni forma de reproducir un entorno.

La cadena, con el CDK:

```
Python (CDK)  ──synth──►  plantilla CloudFormation (JSON)  ──deploy──►  recursos en AWS
```

El paso intermedio importa: la plantilla es un artefacto real que se puede leer,
comparar (`cdk diff`) y probar **antes** de tocar nada. Un error que se ve en la
plantilla cuesta segundos; el mismo error visto en el despliegue cuesta veinte
minutos de rollback.

## Deriva de configuración

La diferencia que aparece entre lo que dice el código y lo que hay de verdad en
la nube, en cuanto alguien toca algo a mano. `cdk diff` (o `terraform plan`) es
lo que la hace visible.

---

# 4. Modelado dimensional

## Modelo estrella

Una **tabla de hechos** en el centro rodeada de **dimensiones**, cada una a un
solo join de distancia. De ahí el nombre.

Lo natural sería normalizar (cliente → ciudad → provincia → país, cada uno en su
tabla). El modelo estrella deliberadamente **no** lo hace, por dos razones:

1. **Menos joins.** Una consulta analítica típica cruza los hechos con 3-5
   dimensiones. Con un esquema normalizado serían quince joins y un plan de
   ejecución imprevisible.
2. **Se entiende.** Alguien de negocio puede leer el modelo y saber qué
   preguntar; un esquema normalizado exige conocer el modelo relacional entero.

Se paga con redundancia —el país se repite en cada fila de la dimensión de
cliente— y compensa: el almacenamiento es barato y las dimensiones son pequeñas.

## Snowflake (copo de nieve)

Variante del modelo estrella en la que las dimensiones sí se normalizan en
varias tablas (`dim_product` → `dim_category` → `dim_department`). Ahorra algo de
espacio y añade joins y complejidad. Rara vez compensa.

## Tabla de hechos vs dimensión

- **Hecho**: algo que **ocurrió**, con medidas numéricas. Muchas filas, crece
  sin parar.
- **Dimensión**: el **contexto** de ese algo —quién, qué, cuándo, dónde—. Pocas
  filas, muchas columnas descriptivas.

Regla práctica: si es un número que quieres sumar, es una medida y va al hecho;
si es un atributo por el que quieres agrupar o filtrar, va a una dimensión.

## Medida aditiva

Una medida que se puede sumar por cualquier dimensión y sigue significando algo.
El importe de una línea lo es.

Un **precio unitario no lo es**: sumar precios no significa nada. Se guarda para
auditar, no para agregar. Y el total de un pedido repetido en cada línea sería
peor todavía, porque **parecería** aditivo sin serlo.

## Clave subrogada vs clave natural

- **Natural** (o de negocio): la del origen, `customer_id = 4821`.
- **Subrogada**: la que inventa el almacén, `customer_key`.

Con SCD tipo 2 la subrogada es **imprescindible**, no un capricho: un mismo
`customer_id` tiene varias filas en la dimensión, una por versión, y el hecho
tiene que apuntar a una concreta.

Conviene calcularlas con un **hash determinista** de la clave de negocio, no con
un contador. Con un contador (`monotonically_increasing_id` y equivalentes),
cada reconstrucción de la dimensión asigna claves distintas y los hechos ya
escritos pasan a apuntar a la fila equivocada, sin que nada falle.

## Dimensión degenerada

Un identificador que vive en la tabla de hechos porque no tiene atributos que
colgar de él. `order_id` en una tabla a grano de línea es el caso típico: sirve
para agrupar las líneas de un mismo pedido y para rastrear, pero una tabla
`dim_order` que solo tuviera el id no aportaría nada.

## Miembro desconocido

La fila especial de cada dimensión —convencionalmente con clave `-1`— a la que
apuntan los hechos cuya dimensión no se encontró.

Existe porque descartar esos hechos **falsearía los totales**: son ventas
reales. Mandarlos al miembro desconocido los conserva y además deja el problema
visible y medible: se puede preguntar «¿cuánto facturamos a clientes
desconocidos?» y perseguirlo.

La clave es negativa a propósito: nunca colisiona con un hash y se ve a simple
vista.

## SCD tipo 1 y tipo 2

*Slowly Changing Dimension*: qué hacer cuando un atributo de una dimensión
cambia.

- **Tipo 1**: se sobrescribe. No hay historia.
- **Tipo 2**: se cierra la versión anterior y se abre una nueva, con
  `valid_from`, `valid_to` e `is_current`.

Elegir uno u otro es **una decisión de negocio, no técnica**. Un producto suele
ser tipo 1: el precio al que se vendió de verdad ya está en la línea de pedido,
que es donde debe estar. Un cliente suele ser tipo 2, porque quieres poder decir
*«cuánto vendimos a clientes premium en marzo»* con el segmento que tenían
**entonces**, no con el de hoy.

No todos los cambios merecen versión nueva. Corregir una errata en un nombre no
cambia ningún análisis; cambiar de segmento sí. La lista de atributos que abren
versión es una decisión explícita.

!!! aviso "El fallo silencioso del SCD2"
    Comparar atributos con `!=` en lugar de una comparación *null-safe*
    (`<=>` en Spark, `IS DISTINCT FROM` en SQL estándar). En SQL,
    `NULL != 'gold'` no es verdadero: es NULL. Así que un cambio **desde** NULL
    no se detecta, la versión no se abre y la historia se pierde sin que nada
    falle ni aparezca en ningún log.

## Join point-in-time (*as-of join*)

Unir un hecho con la versión de la dimensión **vigente en la fecha del hecho**:

```sql
ON  f.customer_id = d.customer_id
AND f.order_date >= d.valid_from
AND f.order_date <  d.valid_to
```

Es para lo que existe el SCD tipo 2. Dos formas de equivocarse, y las dos son
silenciosas:

- Unir solo por la clave natural devuelve **una fila por versión** y multiplica
  los hechos. Los ingresos se inflan y nada falla.
- Unir contra `is_current` da el estado de **hoy**, que es justo lo que no
  quieres al analizar el pasado.

`valid_to` se rellena con una fecha centinela lejana (`9999-12-31`) en vez de
NULL, para que la condición se escriba sin un `OR valid_to IS NULL` que además
impide aprovechar particiones e índices.

---

# 5. Calidad e integridad

## Cuarentena

Apartar las filas que fallan la validación en lugar de descartarlas, guardando
**el motivo** del rechazo.

Descartar en silencio es la forma más rápida de perder la confianza en un data
lake: los números no cuadran y no hay forma de saber por qué. Guardar el motivo
importa tanto como guardar la fila, y conviene guardar **todos** los motivos de
cada fila: si un pedido tiene el importe negativo *y* el cliente huérfano,
enterarse de las dos cosas a la vez evita arreglar una y descubrir la otra al
día siguiente.

## Puerta de calidad (*quality gate*)

Un punto del pipeline donde se decide si lo que viene detrás se ejecuta o no,
según una métrica de calidad medida.

Lo importante es **dónde vive la decisión**. Si el propio trabajo que transforma
los datos decide además si el pipeline sigue, mezcla dos responsabilidades y
hace imposible distinguir dos cosas muy distintas: que el pipeline se pare
porque los datos venían mal (funcionamiento correcto) y que se pare porque algo
se rompió (avería). Con la decisión en el orquestador, cada caso tiene su camino
y su alerta.

## Clave foránea huérfana

Una clave que apunta a algo que no está. Hay **dos casos muy distintos** y
confundirlos sale caro:

- Apunta a algo que **nunca existió** en el origen → dato malo de verdad.
- Apunta a algo que **existía pero no sobrevivió a la limpieza** → el dato es
  correcto; el problema es del padre, no suyo.

## Efecto cascada (amplificación de la cuarentena)

Lo que ocurre al tratar el segundo caso como el primero: al validar la
integridad referencial contra las filas **supervivientes** de la capa limpia, un
padre rechazado arrastra a todos sus hijos, y esos a los suyos.

```
   628 clientes a cuarentena
         ↓ dejan huérfanos
 3.253 pedidos
         ↓ dejan huérfanas
19.303 líneas de pedido
```

Un 1% de huérfanos reales se convierte en un 10% de cuarentena: casi 7× de
amplificación, y la tasa de calidad deja de significar nada.

La solución es validar contra **el universo de claves visto en el origen** —todo
el lote, incluidas las filas que van a cuarentena— en lugar de contra las
supervivientes. Una fila cuyo padre existe pero está en cuarentena no es un dato
malo.

## Referencialmente cerrada

Una capa es referencialmente cerrada si **ninguna fila apunta a algo que no esté
en esa misma capa**.

No serlo puede ser una decisión legítima:

| | Cerrada | No cerrada |
|---|---|---|
| Consistencia interna | garantizada | hay que resolverla aguas abajo |
| Efecto cascada | sí, y amplifica | no |
| Los totales cuadran | no: se pierden hechos | sí |

Quien renuncia a cerrarla hereda el problema en la capa siguiente, y lo resuelve
con el **miembro desconocido** (§4).

## Umbral de calidad

El porcentaje de rechazo a partir del cual un lote se considera inservible.
Conviene que sea **por tabla y declarativo**, no un número global escondido en el
código: cada tabla tiene una tolerancia distinta, y un umbral único obliga a
elegir entre ser laxo con la tabla crítica o histérico con la tolerante.

---

# 6. Formatos y almacenamiento

## Parquet: por qué no CSV

Un CSV guarda los datos **por filas**: para leer una columna hay que recorrer
todas. Parquet los guarda **por columnas**, y eso cambia tres cosas:

1. **Solo se leen las columnas que pides.** En una tabla de 40 columnas, una
   consulta que usa 3 lee menos del 10%.
2. **Comprime mucho mejor.** Los valores de una misma columna se parecen entre
   sí; los de una misma fila no.
3. **Tiene esquema y tipos.** Un CSV no sabe si `007` es un número o un texto,
   ni qué es una fecha. Parquet sí, y guarda además estadísticas por bloque
   (mínimo, máximo) que permiten **saltarse bloques enteros** sin leerlos.

En un motor que cobra por byte escaneado, esas tres cosas son la factura.

## Table format (Iceberg, Delta, Hudi)

Una capa por encima de los ficheros que aporta lo que un directorio de Parquets
no tiene: transacciones, `UPDATE`/`DELETE`, evolución de esquema y viaje en el
tiempo.

El problema que resuelven: sin ellos, «una tabla» es un directorio, y escribir
significa dejar ficheros sueltos. Si el proceso muere a medias, quien lea verá
media escritura. No hay forma de borrar una fila. Y renombrar una columna
implica reescribirlo todo.

Los tres grandes hacen lo mismo por caminos distintos: **Iceberg** es el más
neutral respecto al motor y el que mejor soporta Athena y Glue; **Delta** viene
del mundo Databricks; **Hudi** nació orientado a la ingesta incremental.

Un efecto lateral muy útil de Iceberg en AWS: una tabla **se registra sola en el
Glue Data Catalog**, así que es consultable desde Athena sin declarar nada.

## Upsert (`MERGE INTO`)

Insertar si no existe, actualizar si existe, en una sola operación atómica:

```sql
MERGE INTO tabla t USING lote s ON t.clave = s.clave
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Es lo que hace idempotente a una capa: se puede reprocesar el mismo lote N veces
y el resultado no cambia.

## Partición

Dividir físicamente una tabla por el valor de una columna para no leerla entera.
Es la diferencia entre escanear un día y escanear tres años.

## Partición oculta (*hidden partitioning*)

En el modelo de Hive, particionar por mes obliga a crear una columna `mes` y a
que **quien consulta se acuerde de filtrar por ella**. Si filtra solo por la
fecha, lee la tabla entera; no da error, solo cuesta dinero y nadie se entera.

Iceberg guarda la transformación (`months(order_date)`) como metadato: se filtra
por la fecha y el motor deduce qué particiones tocar. El que consulta no tiene
que conocer el esquema físico.

## Time travel

Consultar el estado de una tabla en un momento anterior, usando los *snapshots*
que el table format escribe en cada commit:

```sql
SELECT * FROM tabla FOR SYSTEM_TIME AS OF '2026-08-05 10:00:00';
```

Convierte una auditoría de tres días en una consulta.

## Snapshot

La foto inmutable de una tabla tras un commit. Un table format no modifica
ficheros: escribe nuevos y publica un snapshot que dice cuáles componen la tabla
ahora. De ahí salen a la vez la atomicidad y el viaje en el tiempo.

## Problema de los ficheros pequeños

Cada escritura crea ficheros nuevos. Con ejecuciones frecuentes se acumulan
miles de ficheros diminutos, y leer 10.000 ficheros de 1 MB es mucho más lento
que leer 100 de 100 MB: el coste está en abrir cada fichero y leer su metadato,
no en los bytes.

Se resuelve compactando periódicamente (`rewrite_data_files` en Iceberg).

## Evolución de esquema

Añadir, renombrar o cambiar el tipo de una columna sin reescribir la tabla ni
romper a quien la consulta. Iceberg lo soporta porque sigue las columnas por un
**id interno**, no por su posición ni por su nombre en el fichero.

## Catálogo de datos

El registro de qué tablas existen, qué columnas tienen y dónde están sus
ficheros. Sin él, un motor de consulta solo ve un bucket lleno de ficheros.

Se puede poblar de dos formas: **declarándolo** (en el código de
infraestructura) o **descubriéndolo** con un *crawler* que husmea los ficheros y
adivina el esquema. Declararlo es determinista y gratis; un crawler cuesta en
cada ejecución, tarda, y sus adivinanzas cambian cuando cambian los datos.

---

# 7. Spark

## Driver y executors

- El **driver** es el proceso que ejecuta tu código, construye el plan y
  reparte el trabajo.
- Los **executors** son los procesos que hacen el trabajo de verdad, cada uno
  sobre un trozo de los datos.

Todo lo que llames desde el driver sobre datos completos (`collect()`,
`toPandas()`) trae esos datos a **una sola máquina**. Es la forma más habitual de
tumbar un trabajo que funcionaba con datos de prueba.

## Partición (en Spark)

El trozo de datos que procesa una tarea. No es lo mismo que una partición de
tabla (§6), aunque estén relacionadas: el número de particiones determina cuánto
paralelismo hay. Demasiadas y el coste de coordinarlas domina; demasiado pocas y
las máquinas están paradas.

## Shuffle

La redistribución de datos entre executors que exigen operaciones como
`groupBy`, `join` o `distinct`: para agrupar por una clave, todas las filas con
esa clave tienen que acabar en la misma máquina.

Es **la operación cara** de Spark, porque implica escribir a disco y mandar
datos por la red. Casi toda la optimización de Spark consiste en tener menos
shuffles o hacerlos más pequeños.

## Evaluación perezosa (*lazy*)

Spark no ejecuta nada al definir una transformación: construye un plan. El
trabajo solo ocurre cuando algo pide un resultado (`count()`, `show()`,
`write`). Eso le permite optimizar el plan completo, y explica por qué un error
aparece en una línea que no tiene nada que ver con la que lo causó.

## Predicate pushdown

Empujar el filtro lo más cerca posible del origen: en vez de traerse toda la
tabla y filtrar en Spark, se manda el `WHERE` a la base de datos o se usan las
estadísticas de Parquet para no leer bloques. Es lo que hace que una extracción
incremental sea barata.

## Ventana (*window function*)

Una función que calcula sobre un conjunto de filas relacionadas sin colapsarlas
en una sola, a diferencia de un `GROUP BY`. `row_number() OVER (PARTITION BY
clave ORDER BY fecha DESC)` es el patrón estándar para **deduplicar** quedándose
con la versión más reciente de cada clave.

---

# 8. Redes e IAM en AWS

## VPC y CIDR

Una **VPC** es una red privada dentro de AWS. Al crearla se le da un rango de
direcciones en notación **CIDR**, por ejemplo `10.20.0.0/16`.

El número tras la barra dice **cuántos bits del principio están fijos**. Con
`/16`, los dos primeros octetos (`10.20`) no cambian y los otros dos son libres:
65.536 direcciones, de `10.20.0.0` a `10.20.255.255`. Cuanto mayor el número,
más pequeña la red: un `/24` son 256 direcciones.

Conviene que sea holgado y que no se solape con otras redes de la organización,
porque cambiarlo después obliga a recrear la VPC entera.

## Subred

Una porción de la VPC, atada a **una** zona de disponibilidad. Se distingue por
cómo sale a internet:

- **Pública**: tiene ruta a un Internet Gateway.
- **Privada con NAT**: sale a internet, pero no es alcanzable desde fuera.
- **Privada aislada**: no sale. Lo más seguro, y lo que obliga a resolver por
  VPC endpoints todo lo que necesite hablar con servicios de AWS.

## Security group

Un cortafuegos a nivel de interfaz de red. Solo tiene reglas de **permitir**, y
tiene **estado**: si permites una conexión saliente, la respuesta vuelve sola
sin necesidad de una regla de entrada.

Un patrón que sorprende la primera vez: un security group que **se referencia a
sí mismo** en una regla. Significa «cualquier recurso que tenga este mismo grupo
puede hablar conmigo», y es lo que necesitan los clústeres cuyos nodos se
comunican entre sí por puertos arbitrarios.

## VPC endpoint

Una puerta desde tu red privada a un servicio de AWS **sin pasar por internet**.

Si la VPC no tiene salida a internet, un servicio sin endpoint sencillamente
**no existe** para lo que corra dentro. El síntoma es un timeout de conexión que
no menciona en ningún momento que falte un endpoint.

Hay dos tipos, y la diferencia es de factura:

| Tipo | Servicios | Coste |
|---|---|---|
| **Gateway** | S3 y DynamoDB | gratis |
| **Interfaz** (PrivateLink) | casi todos los demás | por hora, **por endpoint y por AZ** |

Cuidado con la aritmética: cuatro endpoints de interfaz en dos AZ salen más
caros que el NAT Gateway que se estaba intentando evitar.

## Glue Connection

Pese al nombre, no es «una conexión a una base de datos»: es lo que hace que un
trabajo de Glue **se ejecute dentro de tu VPC**, creando ENIs en tus subredes.

Consecuencias prácticas: solo hace falta en los trabajos que hablan con algo que
vive en la VPC; añade tiempo de arranque; y ata el trabajo a la disponibilidad
de esa red. Los trabajos que solo tocan S3 arrancan antes sin ella.

## IAM: principal, política y rol

- **Principal**: quién actúa. Un usuario, un servicio (`glue.amazonaws.com`) o
  una identidad federada.
- **Política**: un documento que dice qué acciones se permiten sobre qué
  recursos.
- **Rol**: un conjunto de políticas que un principal puede **asumir**
  temporalmente. No tiene contraseña ni clave: se asume y se reciben
  credenciales que caducan.

La diferencia entre un rol y un usuario es la clave del diseño moderno: un
usuario tiene credenciales permanentes que hay que guardar y rotar; un rol, no.

## Trust policy

La política que dice **quién puede asumir un rol** (a diferencia de las que
dicen qué puede hacer una vez asumido). Es la puerta de entrada, y por tanto
donde más caro sale un error: una condición de más restringe de más y todo
falla ruidosamente; una condición de menos abre el rol a quien no debe, y no
falla nunca.

## Principio de mínimo privilegio

Dar exactamente los permisos necesarios y ni uno más. La versión práctica en una
cadena de despliegue: el rol que asume el CI **no despliega nada por sí mismo**;
solo puede asumir los roles de despliegue que ya existen y que tienen sus
permisos acotados y auditados.

---

# 9. Orquestación

## Máquina de estados

Una definición explícita de los pasos de un flujo, las transiciones entre ellos
y qué hacer cuando algo falla. Frente a un script que encadena llamadas, aporta
que el estado de cada ejecución es **observable**: se ve dónde está, por dónde
pasó y dónde murió, sin leer logs.

## Integración síncrona (`.sync`)

La diferencia entre «lanza este trabajo» y «lanza este trabajo **y espera a que
termine**».

Es de los errores más caros de cometer y de los más difíciles de ver, porque no
da ningún error: el orquestador lanza el paso 1, sigue inmediatamente al paso 2,
y el paso 2 lee unos datos que todavía se están escribiendo. El resultado es
incompleto y plausible.

## Reintento con backoff exponencial

Volver a intentar tras un fallo, esperando cada vez más entre intentos (30 s, 60
s, 120 s…). Lo importante es la parte del *backoff*: reintentar de inmediato
contra un servicio que ya está saturado empeora exactamente el problema que se
intenta sortear.

Solo tiene sentido para fallos **transitorios**. Reintentar un fallo
determinista (un error de permisos, un bug) es esperar tres veces para fallar
igual.

## Catch

La rama que se toma cuando un paso falla de forma no controlada. Sin ella, la
ejecución se queda en rojo en una consola que nadie mira. Con ella, alguien se
entera.

Conviene distinguir el fallo **esperado** (los datos no pasaron la puerta de
calidad: el sistema funcionó como debe) del fallo **inesperado** (algo se
rompió). Mezclarlos hace imposible interpretar las alertas.

## Fan-out / Map

Ejecutar el mismo paso en paralelo sobre una lista de elementos. Tiene sentido
cuando cada elemento necesita su propio cómputo, o cuando el fallo de uno no
debe bloquear a los demás. **No** lo tiene cuando un solo proceso ya recorre
todos los elementos por dentro: multiplicar los arranques es más caro y más
lento a cambio de nada.

## Idempotencia del orquestador

Que relanzar una ejecución entera no produzca daño. Es la propiedad que hace
usable un reintento, y solo existe si **cada paso** es idempotente (§3).

---

# 10. Git y entrega

## Git Flow

Un modelo de ramificación con ramas de vida larga y ramas de vida corta:

| Rama | Rol |
|---|---|
| `main` | lo que está en producción. Solo recibe `release/*` y `hotfix/*`. |
| `develop` | integración. De aquí salen las features. |
| `feature/*` | una por trabajo. PR contra `develop`. |
| `release/*` | preparación de versión: número y changelog. |
| `hotfix/*` | sale de `main` para arreglar producción. |

## La doble fusión

La parte de Git Flow que se olvida siempre: una rama `release/*` o `hotfix/*` se
fusiona **a `main` y también de vuelta a `develop`**.

No es burocracia. Si solo se fusiona a `main`:

- el arreglo de un hotfix **no está** en `develop`, así que la siguiente versión
  reintroduce el bug —y con un tag posterior, de modo que parecerá una regresión
  nueva y nadie mirará el hotfix;
- el número de versión y el changelog de una release quedan solo en `main`, y las
  dos ramas divergen para siempre en esos ficheros.

## Merge commit vs squash

Un **squash** aplasta todos los commits de una rama en uno solo. Es cómodo para
una feature, donde el historial intermedio no interesa a nadie.

Es una mala idea al fusionar una release a `main`: el squash crea un commit
nuevo sin relación con los originales, así que `main` deja de compartir historia
con `develop` y la fusión de vuelta se convierte en un conflicto de todos los
cambios a la vez.

## Versionado semántico (SemVer)

`MAYOR.MENOR.PARCHE`. Se sube el **parche** al arreglar sin cambiar el
comportamiento esperado, el **menor** al añadir funcionalidad compatible, y el
**mayor** al romper compatibilidad. Comunica a quien actualiza cuánto riesgo
corre antes de mirar el diff.

## Changelog

El registro de cambios pensado para **quien usa** el proyecto, no para quien lo
escribió. Por eso se escribe a mano: un changelog generado de los mensajes de
commit produce una lista de cambios de ficheros, que es justo lo que el lector no
necesita.

## Branch protection y status checks obligatorios

Reglas del servidor sobre una rama: exigir PR, prohibir el *force push*, y
exigir que ciertas comprobaciones automáticas pasen antes de poder fusionar.

!!! aviso "El bloqueo circular"
    Marcar un check como obligatorio **antes** de que exista el workflow que lo
    publica deja todos los PR esperando eternamente un resultado que nadie va a
    reportar. La única salida es desactivar la protección. Primero se fusiona el
    workflow, después se exige.

## Federación de identidad y OIDC

La alternativa a guardar claves de acceso en un sistema de CI.

Con claves: se crea un usuario, se le saca una clave permanente y se pega en los
secretos del repositorio. No caduca, vive en un sistema de terceros, y no hay
forma de saber quién la ha copiado.

Con **OIDC**: el sistema de CI firma un token de vida cortísima que describe
quién está ejecutando qué; la nube lo valida contra el certificado público del
emisor y devuelve credenciales temporales. No hay nada que rotar porque no hay
nada guardado.

## Claims `sub` y `aud`

Los dos campos del token que deciden si se acepta:

- **`aud`** (*audience*): para quién se emitió el token.
- **`sub`** (*subject*): quién es. En GitHub Actions codifica el repositorio y
  el contexto: `repo:owner/repo:ref:refs/heads/main`,
  `repo:owner/repo:environment:prod`.

!!! aviso "El error que abre la cuenta"
    El emisor y el `aud` son **idénticos para todos los repositorios** del
    proveedor. Una política de confianza que solo compruebe esos dos acepta a
    cualquiera: basta con crear un repositorio y copiar el ARN del rol, que
    normalmente está escrito en el propio workflow. En los registros de
    auditoría se ve una autenticación perfectamente legítima.

    El error opuesto es igual de común: un comodín en el `sub`
    (`repo:owner/repo:*`) incluye los eventos de *pull request*, y el token de
    un PR se emite contra el repositorio base, así que un PR desde un fork
    desplegaría con ese rol. Los valores van enumerados y literales.

## Environment protection rule

Una puerta de aprobación manual asociada a un entorno de despliegue. En GitHub
Actions tiene un efecto doble que conviene entender: declarar el entorno en un
job activa la aprobación **y** añade el claim correspondiente al token OIDC.

De ahí sale un diseño que **falla cerrado**: si el rol de producción solo confía
en el claim del entorno y no en la rama, olvidarse de declarar el entorno no
salta la aprobación — impide desplegar.

## Bootstrap (CDK)

La preparación única de una cuenta y región para poder desplegar con el CDK:
crea un bucket para los artefactos y un conjunto de roles con nombres
predecibles. Todo despliegue posterior asume esos roles, lo que permite que la
identidad que lanza el despliegue tenga permisos mínimos.

## Huevo y gallina en el despliegue

El recurso que hace falta para poder desplegar no se puede desplegar con el
mecanismo que él mismo habilita. Se resuelve creándolo una vez a mano y
dejándolo explícitamente fuera del ciclo normal de creación y borrado.

---

# 11. Modos de fallo genéricos

Errores que no dan error. Todos comparten la misma forma: el sistema informa de
éxito y el resultado está mal. Son los caros.

## El CI que da confianza falsa

Una suite que se **salta** las pruebas que no puede ejecutar en lugar de fallar.
El código de salida sigue siendo cero, el check sale verde, y nadie mira el
número de pruebas ejecutadas.

Es peor que no tener CI, porque un CI ausente se nota y uno mentiroso no. La
defensa es hacer que el salto sea un fallo **en el entorno donde saltarse algo
es inaceptable**, y comprobar que el mecanismo funciona provocándolo a mano.

## Deriva de catálogo

El proceso escribe los datos correctamente en el almacenamiento, pero los
registra en un catálogo distinto del que consulta todo el mundo —o en ninguno—.
Los ficheros están, el trabajo terminó bien, y las tablas no aparecen.

Se manifiesta cuando alguien intenta consultar, que puede ser semanas después.
La defensa es que el proceso **diga en su log a qué catálogo está escribiendo**:
convierte un fallo invisible en una línea que se lee de un vistazo.

## Detección de entorno mal hecha

Código que se comporta distinto según dónde corra y que decide dónde corre
mirando algo poco fiable —una variable de entorno que no existe, un fichero que
puede faltar—. Como la rama por defecto suele ser la de desarrollo, el fallo se
traduce en «funciona en local, y en producción hace otra cosa sin quejarse».

## Recursos huérfanos que bloquean un borrado

Un servicio gestionado crea recursos dentro de tu red (interfaces de red,
típicamente) y tarda en liberarlos. Si se intenta borrar la red antes, el
borrado falla y deja el conjunto en un estado del que no se puede salir
reintentando: hay que localizar y borrar los recursos huérfanos a mano.

La defensa es de operación, no de código: esperar unos minutos entre la última
ejecución y el borrado.

## Agregación al grano equivocado

Calcular una media dividiendo entre el número de filas cuando el denominador
correcto es el número de entidades distintas. El resultado es un número
plausible, del orden de magnitud esperado, y sistemáticamente sesgado.

No da ningún error: el trabajo termina bien, el esquema es correcto y la tabla
se escribe. Solo se detecta cuadrando cifras contra otra fuente, y por eso la
única defensa real es una prueba con datos elegidos para que las dos respuestas
posibles **no puedan confundirse**.

## Multiplicación silenciosa por un join

Unir con una tabla que tiene varias filas por clave —una dimensión con historia,
por ejemplo— sin acotar cuál. Cada hecho se duplica una vez por versión. Los
totales se inflan, nada falla, y el error crece con el tiempo a medida que se
acumulan versiones.

## Comparación con NULL

`NULL = NULL` no es verdadero y `NULL != 'x'` no es verdadero: los dos son NULL,
que en un `WHERE` se comporta como falso. Cualquier lógica de detección de
cambios escrita con `=` o `!=` ignora silenciosamente todo lo que entre o salga
de NULL. Se evita con comparaciones *null-safe* (`IS DISTINCT FROM`, `<=>`).

## El comentario que estorbaba

Un comentario que advierte de un error concreto se borra al «mejorar» el código
que documentaba, y con él se va la única defensa que había. Es el patrón por el
que un bug ya conocido vuelve.

La lección no es escribir más comentarios: es que **una advertencia en prosa no
es una defensa**. Lo que impide que un error vuelva es una prueba.


---

# 12. Series temporales y flujos de eventos

## Historian

Un almacén de medidas: una fila por sensor y por instante, para siempre, y **no
se actualiza jamás**. Es la base de datos característica del sector energético
—contadores, telemetría de plantas, SCADA— y su forma es la de cualquier flujo
de eventos.

Lo que lo distingue de una tabla de entidades no es el volumen, es la
**semántica**: en una tabla de entidades una fila representa algo que existe y
cambia; en un historian representa algo que pasó y ya no cambia nunca.

## Tiempo del evento y tiempo de proceso (*event time* vs *processing time*)

Los dos tiempos que trae todo hecho: cuándo **ocurrió** y cuándo **llegó**.
Coinciden casi siempre, y cuando no coinciden, la diferencia es donde vive todo
el problema.

Sirven para cosas distintas y no son intercambiables:

| | Se usa para | Si se usa para lo otro |
|---|---|---|
| Tiempo del evento | Particionar, agregar, todo el análisis de negocio | La extracción incremental **pierde** lo que llega tarde |
| Tiempo de proceso | La extracción incremental | Los agregados de negocio se descolocan de día |

!!! clave "El fallo, en una frase"
    Con el tiempo del **evento** como marca de la extracción, un hecho de ayer
    que llega hoy nace ya por detrás de la marca y no se lee nunca. No falla
    nada: simplemente no está.

## Datos que llegan tarde (*late-arriving data*)

Hechos que llegan después de que su periodo se haya cerrado: un móvil sin
cobertura que sincroniza al recuperarla, un contador que se lee al mes
siguiente, una corrección del emisor.

No son una anomalía a filtrar, son el caso normal de cualquier origen
distribuido. Obligan a decidir explícitamente hasta cuándo se acepta un dato
atrasado, y qué se hace con el agregado que ya se publicó.

## Ventana de reproceso (*lookback window*)

Retroceder la marca de agua un margen fijo en cada ejecución, para volver a leer
un solapamiento. Es lo que impide que un dato atrasado se pierda para siempre.

El precio son duplicados deliberados aguas arriba, y es un precio pequeño
cuando la capa cruda es *append-only* y la siguiente deduplica. La asimetría lo
justifica entero: **releer cuesta segundos, perder datos cuesta una auditoría**.

## Entrega «al menos una vez» (*at-least-once*)

Garantía de casi todo sistema de mensajería: un hecho puede llegar repetido,
pero no puede perderse. La alternativa —«como mucho una vez»— pierde datos, y
«exactamente una vez» es carísima y a menudo una ilusión.

La consecuencia práctica es que la deduplicación **no es opcional** y tiene que
apoyarse en un identificador que genere el emisor, no el receptor.

Ponerle una clave única a la tabla de entrada parece la solución y no lo es:
convierte un duplicado en un error de inserción, y si el duplicado era el bueno,
lo pierdes.

## Sesionización

Reconstruir una visita a partir de hechos sueltos, agrupando por actor y
partiendo allí donde hay un hueco de inactividad mayor que un umbral. Es el
patrón de «islas y huecos»: `lag` para medir el hueco, una marca en los que lo
superan, y una suma acumulada que identifica cada isla.

El identificador de sesión que manda el cliente **no delimita una visita**: vive
en una cookie, y una cookie dura mucho más. Agrupar por él da sesiones de horas
con pausas de horas dentro, y unas métricas creíbles y falsas.

## *Downsampling*

Reducir la resolución de una serie agregándola a un intervalo mayor: de
diez-minutal a horaria, de horaria a diaria. Es lo que hace manejable un
historian, y lo que hay que hacer con cuidado, porque **la media de las medias
no es la media**.

---

# 13. Zona horaria

## Hora local y UTC

Regla que no tiene excepciones útiles: **se guarda en UTC y la hora local es una
vista**. En UTC el tiempo avanza siempre, sin saltos ni repeticiones, y por eso
es lo único sobre lo que se puede agrupar, deduplicar y comparar sin sorpresas.

## Hora ambigua

El día que el reloj se atrasa, una hora local **ocurre dos veces**, separadas por
una hora real. Dos hechos con la misma hora local no son un duplicado: son
momentos distintos. Deduplicar o agrupar por hora local se come la mitad, y no
falla nada.

## Hora inexistente

El día que el reloj se adelanta, una hora local **no ocurre ninguna vez**. Una
marca de tiempo ahí es imposible, y aun así la conversión a UTC devuelve algo en
vez de fallar.

!!! aviso "No es el mismo problema del revés"
    Una duplica y la otra hace imposible, y la implementación evidente las
    confunde: en Python el atributo `fold` cubre las dos, así que comparar los
    desfases devuelve cierto en ambos casos. Lo que las separa es el **signo** de
    la diferencia.

## Días de 23 y 25 horas

En el sector energético esto llega a ser normativo: los ficheros de medidas
horarias declaran un día de 23 horas y otro de 25, con una columna extra que
dice cuál de las dos lecturas de las 02:00 es. Cualquier código que dé por hecho
que un día tiene 24 horas está mal dos días al año.

---

# 14. Ficheros de intercambio

## Fichero multi-registro

Un fichero con **varios esquemas dentro**, distinguidos por el primer campo:
cabecera, detalles y pie. Leerlo como un CSV no da error: da columnas
desplazadas y filas basura mezcladas con las buenas.

Es la forma habitual de los ficheros regulados del sector energético, y su
motivo es histórico: nacieron para cinta y para transmisiones que se podían
cortar.

## Pie de control (*trailer*)

El último registro, con el recuento y las sumas de lo que el emisor dice haber
mandado. Es la **única prueba de integridad** que existe en un fichero plano: uno
truncado se lee perfectamente, porque las líneas que llegaron están bien
formadas.

Conviene comprobar las tres cosas y no solo el recuento. Un fichero con todos
los registros pero mal sumado es peor que uno truncado: significa que el emisor
y tú no estáis mirando lo mismo.

## Zona de aterrizaje (*landing zone*)

El sitio donde un tercero deposita ficheros y del que tú los recoges. Se
conserva el original tal cual llegó, aunque su contenido ya esté cargado: ante
una reclamación hay que poder enseñar exactamente lo que mandó el proveedor,
byte a byte.

## Cuarentena de lote

Apartar un **fichero entero** en vez de fila a fila. Es lo correcto cuando la
integridad se declara a nivel de fichero: media liquidación no es medio dato
bueno, es un total que no cuadra.

## Idempotencia por contenido

Identificar un lote por el **hash de su contenido** y no por su nombre. Es lo
único que reconoce una reexpedición bajo otro nombre, que es como llegan de
verdad las reenvíos.

Conviene registrar solo los lotes **aceptados**: uno rechazado tiene que poder
reintentarse cuando el emisor lo mande completo.

## Mojibake

El texto que resulta de decodificar bytes con la codificación equivocada:
`dañado` que se convierte en `da?ado`. No lanza ninguna excepción con la
configuración por defecto de casi todo, así que viaja intacto hasta el informe
final.

## Fecha ambigua

`03/04/2026` es el 3 de abril o el 4 de marzo según el formato. Leerla con el
equivocado **no falla ningún día del mes menor o igual que 12**, con lo que el
error parece intermitente y se descarta como cosa rara.

---

# 15. Conciliación

## Conciliación

Cuadrar lo que dice tu sistema con lo que dice un tercero. Es la única
comprobación que puede detectar un fallo que ninguna de las dos fuentes ve por
separado, porque **cada una sigue siendo internamente coherente**.

Todo lo demás —cuadres entre capas, tests, puertas de calidad— compara el
sistema consigo mismo.

## Periodo cubierto

El rango que el tercero ha liquidado de verdad. Conciliar fuera de él produce un
muro de falsas alarmas: todo lo anterior al primer fichero sale descuadrado al
100%.

!!! clave "El modo de fallo de casi toda alarma de calidad"
    No es que no detecte. Es que detecta tanto que deja de mirarse.

## Cuadre bruto y cuadre neto

El bruto compara importes antes de deducciones; el neto, después. Meter
comisiones y devoluciones en el cuadre hace que **todos** los días salgan
descuadrados por motivos perfectamente normales, y la señal deja de servir para
lo único que importa: detectar dinero que falta o que sobra.

## Dimensión conformada

Una dimensión compartida por varios hechos, a granos distintos. Es lo que
permite cruzar dos procesos de negocio en la misma pregunta sin mantener dos
definiciones que acabarían divergiendo.

Su contrario es la dimensión duplicada: dos tablas que se llaman igual, se
parecen, y no dicen lo mismo.

## Matriz de bus (*bus matrix*)

La tabla de Kimball que cruza procesos de negocio con dimensiones, y marca
cuáles comparte cada uno. Es la forma de ver de un vistazo qué preguntas se
pueden responder cruzando dos hechos, y cuáles no.

---

# 16. Más modos de fallo genéricos

## El registro heterogéneo

Un registro declarativo del que todo el mundo itera, y al que se le añade una
entrada de otra naturaleza. Todos los consumidores siguen compilando y todos
revientan en ejecución, cada uno por su lado.

No hay test que lo cace de la forma habitual: el fallo no está en una función
que se pueda invocar, está **en la línea que elige sobre qué iterar**.

El arreglo no es una lista de excepciones —que hay que acordarse de actualizar
en cada sitio— sino que cada consumidor declare con qué tipos sabe tratar.

## El estado externo que sobrevive al borrado

Marcas de agua, punteros de *offset*, registros de control: estado que describe
un origen y que vive **fuera** de él. Al reconstruir el origen, ese estado sigue
ahí describiendo algo que ya no existe.

El síntoma es que la extracción informa de «sin cambios», exactamente lo que
informaría un día tranquilo, y el pipeline sigue corriendo sobre datos viejos
hasta que alguien cuadra totales y no le salen.

La regla: **quien invalida el estado debe ser quien lo rompe**, no quien se
acuerde de llamarlo después.

## El umbral que se cumple por los pelos

Un límite de calidad que la medición real roza exactamente —4,00% con el umbral
en 4%— no está calibrado, ha tenido suerte. Fallará sin que haya cambiado nada.

Un umbral se pone a partir de la línea base observada **más un margen**, y el
margen es la parte que casi nadie escribe.

## El tipo que el motor no modela

Spark no tiene tipo UUID; lo lee y lo escribe como texto, y el driver de destino
se niega a convertirlo aunque el valor sea válido. La familia es más amplia:
intervalos, tipos geométricos, enumerados nativos, `jsonb`.

Solo aparece la primera vez que una tabla usa uno de ellos, y hasta entonces
todo funciona.
