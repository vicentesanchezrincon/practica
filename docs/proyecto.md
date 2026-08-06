<div class="portada" markdown="1">

# practica

<p class="sub">Pipeline de datos medallion: de Postgres a un modelo estrella</p>

<p class="meta">
AWS Glue 5.0 · PySpark · Apache Iceberg · Step Functions · AWS CDK<br>
Documentación del proyecto
</p>

</div>

<div class="toc" markdown="1">

# Índice

[TOC]

</div>

# 1. Qué es este proyecto

Un pipeline de datos completo, de punta a punta, construido para practicar Data
Engineering y DataOps sobre infraestructura real: extrae de una base de datos
Postgres en AWS, la deposita en S3 en tres capas de calidad creciente y acaba en
un modelo dimensional consultable con SQL. Todo desplegado con infraestructura
como código, orquestado, probado y con entrega continua.

!!! nota "Los conceptos, aparte"
    Este documento cuenta **este proyecto**: qué se construyó, qué se decidió y
    qué se rompió. Los conceptos generales —qué es un modelo estrella, qué es
    una VPC, qué significa cada sigla— están en el documento hermano,
    `glosario.pdf`.

## 1.1 El problema que resuelve

Una base de datos de operación diaria está diseñada para responder rápido a
«dame el pedido 48219». No está diseñada para «dame los ingresos por categoría y
mes de los últimos tres años», que es una consulta que recorre millones de filas
y que, lanzada contra producción, la deja de rodillas justo cuando los clientes
están comprando.

La solución estándar es sacar los datos a otro sitio y transformarlos allí. Eso
plantea tres preguntas que este proyecto responde:

1. **Cómo sacarlos sin releerlo todo cada noche.** → extracción incremental por
   watermark (cap. 8).
2. **Cómo limpiarlos sin perder lo que se descarta.** → cuarentena con motivo
   (cap. 9).
3. **Cómo dejarlos en una forma que alguien pueda consultar.** → modelo estrella
   (cap. 10).

## 1.2 El recorrido de un dato

Un pedido que alguien confirma en la tienda hace este viaje:

```
RDS Postgres ──JDBC──► bronze_ingest ──► s3://…/bronze/  Parquet crudo, inmutable
                                              │
                            silver_transform ──► s3://…/silver/  Iceberg, MERGE
                                              │
                                  ¿calidad OK? ──no──► SNS, se detiene
                                              │ sí
                                  gold_build ──► s3://…/gold/  modelo estrella
                                              │
                                        Athena / Glue Data Catalog
```

1. **Bronze.** El job pregunta a Postgres «¿qué ha cambiado desde la última
   vez?» y escribe la respuesta tal cual, sin tocar nada, añadiendo solo tres
   columnas de linaje.
2. **Silver.** Se normaliza, se deduplica, se valida y se fusiona con lo que ya
   había. Lo que no pasa la validación se aparta, no se tira.
3. **La puerta de calidad.** Si se rechazó demasiado, el pipeline se detiene
   aquí y avisa. Gold no se construye sobre un lote del que se perdió media
   tabla.
4. **Gold.** El pedido se convierte en filas de una tabla de hechos, unidas a la
   versión del cliente que estaba vigente **el día del pedido**.

## 1.3 Las dos mitades: contenedor y host

El proyecto vive en dos entornos que no se mezclan, y entenderlo evita la mayor
parte de la confusión con el `Makefile`:

| Dónde | Qué corre | Por qué |
|---|---|---|
| **Contenedor de Glue** | PySpark, tests, generador de datos, `ruff` | Es la misma imagen que ejecuta AWS: mismo Spark, mismo Python, mismos JAR de Iceberg |
| **Host** | CDK, AWS CLI, `gh`, generación de la documentación | Necesitan Node, tus credenciales de AWS y tu navegador |

En el `Makefile` la frontera es literal:

```make
IN_GLUE := $(COMPOSE) exec -T glue bash -lc
```

Todo lo que lleva ese prefijo entra al contenedor. Es `bash -lc` y no `bash -c`
porque la imagen define `SPARK_HOME` y el `PATH` en el perfil de login: sin
`-l`, `spark-submit` no se encuentra.

---

# 2. Estructura del repositorio

```
practica/
├── .github/workflows/       ci.yml, deploy-dev.yml, deploy-prod.yml
├── data_generator/          schema.sql, seed.py, export_seed.py
├── docs/                    proyecto.md, glosario.md, estilo.css, build_docs.py
├── infra/                   AWS CDK en Python
│   ├── app.py               punto de entrada: cablea los stacks
│   ├── stacks/              network, storage, database, glue, orchestration, cicd
│   └── tests/               65 tests sobre la plantilla generada
├── local/                   docker-compose.yml, Dockerfile
├── src/
│   ├── common/              codigo compartido, se empaqueta para Glue
│   └── jobs/                seed_rds, bronze_ingest, silver_transform, gold_build
├── tests/unit/              104 tests con PySpark local
├── CHANGELOG.md
├── Makefile
└── pyproject.toml
```

Cuatro decisiones de estructura que merecen explicación:

- **`src/common` está separado de `src/jobs`.** Los jobs son guiones que Glue
  ejecuta; `common` es una librería que se empaqueta en un `.zip` y se sube
  aparte. Esa separación es lo que permite probar la lógica sin arrancar un job.
- **Los tests de infraestructura viven en `infra/tests`, no en `tests/`.**
  Necesitan `aws-cdk-lib`, que no tiene nada que hacer en el contenedor de
  Spark. Son dos suites con dos entornos distintos.
- **`data_generator` no está dentro de `src`.** No forma parte del pipeline: es
  andamiaje para tener datos con los que trabajar.
- **`infra/cdk.context.json` está en `.gitignore`.** Cachea el id de la cuenta
  de AWS, y **este repositorio es público**.

---

# 3. El entorno local

El objetivo de esta parte es poder equivocarse gratis. Todo el desarrollo de
PySpark ocurre contra un Postgres en Docker y un directorio local, sin gastar un
céntimo en AWS.

## 3.1 `docker-compose.yml`

Dos servicios.

**`postgres`**: la imagen oficial `postgres:16-alpine`, con volumen persistente
y un *healthcheck*, para que `make seed` no arranque antes de que la base esté
lista.

**`glue`**: la imagen `public.ecr.aws/glue/aws-glue-libs:5`, que es la misma que
usa AWS. Verificado dentro de ella: **Spark 3.5.2, Python 3.11.15, Iceberg
1.7.1-amzn-1**. (La documentación de AWS anuncia Spark 3.5.4; el tag `:5` va
ligeramente por detrás. Es el tipo de detalle que solo se descubre mirando.)

!!! aviso "El contenedor que se moría al arrancar"
    La imagen define `ENTRYPOINT ["bash", "-l"]`. Poner
    `command: ["sleep", "infinity"]` acaba ejecutando `bash -l sleep infinity`,
    que es un error de sintaxis, así que el contenedor sale inmediatamente sin
    decir por qué. Lo correcto es `command: ["-c", "sleep infinity"]`.

## 3.2 El Dockerfile, y por qué añade tan poco

`local/Dockerfile` extiende la imagen oficial con tres paquetes: `faker`,
`psycopg` y `ruff`. Nada más.

Es deliberado. Cada cosa que se añade a la imagen es una diferencia con lo que
AWS ejecuta de verdad, y las diferencias entre el entorno de desarrollo y el de
producción son exactamente los bugs que aparecen tarde y caros. Los tres
paquetes que se añaden son herramientas de desarrollo, no dependencias del
pipeline.

## 3.3 El problema de uid y gid

El usuario por defecto de la imagen de Glue es `hadoop`, con **uid 10000**. Tu
usuario del host tiene otro. Al montar el proyecto como volumen, el contenedor
no puede escribir en tus ficheros.

Se resuelve arrancándolo con tu grupo:

```yaml
user: "10000:${HOST_GID:-1000}"
```

Y en el `Makefile`:

```make
export HOST_GID := $(shell id -g $$(id -un))
```

!!! aviso "Por qué no vale `id -g` a secas"
    `id -g` devuelve tu grupo **primario**, y dentro de una shell abierta con
    `newgrp docker` ese grupo pasa a ser `docker`. El contenedor arrancaría con
    el gid de docker y volvería a no poder escribir. `id -g $(id -un)` consulta
    `/etc/passwd` y devuelve siempre el grupo real.

    El mismo problema tiene una segunda cara: los ficheros que **tú** crees
    dentro de una shell con `newgrp docker` nacen con grupo `docker`, y entonces
    es el contenedor el que no puede tocarlos. Se arregla con `chgrp`.

---

# 4. Los datos: esquema y generador

## 4.1 El esquema

Cuatro tablas en el esquema `ecommerce`: `customers`, `products`, `orders` y
`order_items`. Suficiente para tener una jerarquía de dos niveles (pedido →
línea) y dos dimensiones, que es lo mínimo para que un modelo estrella tenga
sentido.

Todas llevan `created_at` y `updated_at`. La segunda es la que hace posible la
extracción incremental, y se mantiene sola con un *trigger*:

```sql
CREATE TRIGGER touch_updated_at BEFORE UPDATE ON ...
```

Sin el trigger, la columna dependería de que cada `UPDATE` de la aplicación se
acordara de tocarla. Se olvidaría, y esas filas no se volverían a extraer nunca.

## 4.2 La suciedad deliberada

`seed.py` inyecta defectos a propósito, controlados por una clase `DirtRates`:
correos con mayúsculas y espacios, duplicados exactos, nulos en campos
obligatorios, importes negativos, códigos de país mal formados y claves foráneas
huérfanas.

Es lo que da trabajo a la capa Silver. Un generador que produjera datos
perfectos no permitiría probar nada de lo interesante.

El factor se puede subir para provocar el corte de la puerta de calidad:

```bash
python3 data_generator/seed.py --mode daily --dirt-factor 10
```

## 4.3 Los modos `initial` y `daily`

- `--mode initial`: carga histórica (20.000 clientes, 2.000 productos, 80.000
  pedidos).
- `--mode daily`: simula un día — altas nuevas **y modificaciones sobre filas
  antiguas**, tocando su `updated_at`.

Las modificaciones son la parte importante: sin ellas, el incremental solo se
probaría con inserciones y no se vería la diferencia entre «filas nuevas» y
«filas que cambiaron».

!!! aviso "Dos bugs del generador que habrían falseado las pruebas"
    El modo `daily` colgaba las líneas nuevas de pedidos antiguos elegidos al
    azar (`ORDER BY random() LIMIT`). Y ponía `updated_at = now()` en
    `order_items` en lugar de heredar el del pedido padre.

    El segundo era el grave: habría hecho que la primera extracción incremental
    se trajera el histórico entero de líneas, y la prueba de que el incremental
    funciona habría salido «bien» por el motivo equivocado.

---

# 5. El código compartido: `src/common`

## 5.1 `config.py`: la configuración como datos

La pieza central del proyecto. Cada tabla se describe con un `TableSpec`
declarativo, y **añadir una tabla al pipeline es añadir una entrada a un
diccionario**, sin escribir una línea de Spark:

```python
"customers": TableSpec(
    business_key=["customer_id"],
    watermark_column="updated_at",
    lower_trim=["email"],
    upper_trim=["country_code"],
    not_null=["customer_id", "email"],
    patterns={"country_code": r"^[A-Z]{2}$"},
    quarantine_threshold=0.06,
),
```

Los tres jobs leen de aquí. Eso significa que la clave de negocio que usa el
`MERGE` de Silver y la que usa la deduplicación son **la misma por
construcción**, no por acuerdo entre dos ficheros que alguien tiene que mantener
sincronizados.

`INGESTION_ORDER` fija el orden de las tablas: los padres antes que los hijos.
No es cosmético — la validación de integridad referencial necesita que el padre
esté procesado.

## 5.2 `spark_session.py`: una sesión para dos mundos

El mismo código tiene que funcionar en el contenedor local (sin Glue Data
Catalog, warehouse en disco) y en un job real (catálogo de Glue, warehouse en
S3). `build_session()` detecta dónde está y configura lo que toca, de modo que
los jobs no tienen ni un `if` de entorno dentro de la lógica.

!!! aviso "El bug más caro del proyecto"
    La detección **nunca funcionó**. Miraba una variable de entorno que no
    existe en Glue 5.0 y, además, hacía `os.getenv("--JOB_NAME")`, que es un
    argumento de línea de comandos y no una variable.

    Consecuencia: los jobs escribían las tablas Iceberg con el catálogo
    **Hadoop** en lugar del de Glue. Los datos quedaban perfectamente en S3 pero
    **no se registraban en ningún sitio**, así que Athena no las veía. Todo
    parecía funcionar hasta que alguien intentó consultar.

    La versión correcta mira `sys.argv`, que es donde Glue inyecta `--JOB_NAME`.
    Y, sobre todo, ahora **lo dice en el log**: un `print` con el catálogo
    elegido convierte un fallo invisible en una línea que se lee de un vistazo.

También resuelve los JAR: la imagen de Glue trae Iceberg y el driver de Postgres
pero **fuera del classpath de Spark**. En un job real los añade la plataforma;
en local hay que buscarlos con un glob y añadirlos a mano, o el primer
`CREATE TABLE ... USING iceberg` falla con `ClassNotFoundException`.

## 5.3 El resto

| Módulo | Qué hace |
|---|---|
| `watermark.py` | Lee y escribe los watermarks en SSM Parameter Store |
| `jdbc.py` | Credenciales desde Secrets Manager y ejecución de SQL vía JDBC |
| `transforms.py` | Normalización y deduplicación por ventana |
| `quality.py` | Parte un DataFrame en válidas y cuarentena, con los motivos |
| `dimensions.py` | Claves subrogadas, miembro desconocido, *as-of join*, SCD2 |

---

# 6. La infraestructura: los seis stacks

`infra/app.py` es el punto de entrada. Un solo app para dos entornos, elegidos
por contexto:

```bash
cdk deploy --all -c environment=dev
```

Los stacks quedan como `Practica-Dev-Network`, `Practica-Prod-Storage`, de modo
que dev y prod conviven en la misma cuenta sin pisarse.

| Stack | Qué crea |
|---|---|
| `Network` | VPC 10.20.0.0/16, solo subredes aisladas, security groups, VPC endpoints |
| `Storage` | Bucket del data lake y las tres bases del Glue Data Catalog |
| `Database` | RDS Postgres 16.9 `db.t4g.micro`, secreto y Glue Connection |
| `Glue` | Rol de ejecución, publicación de scripts a S3 y los cuatro jobs |
| `Orchestration` | Máquina de estados, topic SNS y regla de EventBridge |
| `Cicd` | Proveedor OIDC de GitHub y los dos roles de despliegue |

## 6.1 Red: una VPC sin salida a internet

Solo subredes **aisladas**: sin Internet Gateway y **sin NAT Gateway**. El NAT
cuesta unos 33 USD al mes fijos y es el error de coste número uno en proyectos
de datos.

A cambio, todo lo que los jobs necesitan tiene que llegar por VPC endpoint.

!!! aviso "El endpoint que faltaba"
    El job de Bronze moría con `ConnectTimeoutError` **tras dos minutos** de
    espera, sin mencionar en ningún momento que faltase un endpoint de SSM.

    Ahora hay un test de regresión que enumera todos los servicios que usan los
    jobs y comprueba que cada uno tiene su endpoint. Es la clase de fallo que no
    se puede permitir descubrir dos veces.

!!! aviso "La cuenta de los endpoints estaba mal hecha"
    Los endpoints de **interfaz** se pagan por endpoint **y por AZ**. Cuatro
    endpoints en 2 AZ salen ~58 USD/mes: más caro que el NAT Gateway que se
    estaba evitando.

    Se corrigió desplegándolos en **una sola AZ** (el tráfico entre AZ de
    PrivateLink es gratis desde 2022) y añadiendo un interruptor para apagarlos:
    `-c interface_endpoints=false`.

El security group de Glue **se referencia a sí mismo** en todos los puertos. No
es un descuido: los nodos del clúster de Spark se hablan por puertos arbitrarios
y Glue lo exige. Sin esa regla, el job se cuelga y muere por timeout sin decir
por qué.

## 6.2 Almacenamiento

Un solo bucket con prefijos por capa, no tres buckets: las políticas de acceso
se hacen igual de bien por prefijo.

Las bases del catálogo se **declaran**, no se descubren con un crawler. Un
crawler cuesta en cada ejecución, tarda, y adivina el esquema.

Regla de ciclo de vida relevante: Iceberg reescribe metadatos constantemente y
el bucket tiene versionado, así que sin una regla que expire las versiones
antiguas la factura crece sola.

## 6.3 Base de datos

RDS Postgres en las subredes aisladas. **La contraseña no existe en el código**:
la genera AWS y vive en Secrets Manager. En la plantilla de CloudFormation solo
aparece como `{{resolve:secretsmanager:...}}` y en los argumentos de los jobs
solo viaja el *nombre* del secreto.

La **Glue Connection** es lo que mete los jobs dentro de la VPC. Sin ella el job
corre en la red de AWS y no ve el RDS.

## 6.4 Un detalle de cableado

CloudFormation deduce casi todas las dependencias de las referencias cruzadas,
pero el job declara la Connection **por nombre**, que es un string, no una
referencia. Sin una dependencia explícita, Glue podría desplegarse antes de que
la Connection exista:

```python
glue_stack.add_stack_dependency(database)
```

---

# 7. La siembra del RDS: el rodeo por S3

El RDS vive en subredes aisladas, así que **no se alcanza con `psql` desde tu
portátil**. Eso es deliberado: es el mismo problema que hay en cualquier empresa
que tenga la red bien montada.

La ruta:

```
Postgres local ──export_seed──► Parquet ──s3 sync──► S3 ──job de Glue──► RDS
```

```bash
make export-seed     # exporta a Parquet y sube a S3
make seed-rds        # lanza el job de Glue y espera
```

El job aplica el **mismo** `data_generator/schema.sql` que se usa en local: se
sube a S3 en cada despliegue, así que no hay dos copias que puedan divergir.

Después de cargar, recoloca las secuencias con `setval`. Los IDs se insertan
explícitamente, así que las secuencias seguirían en 1 y el primer `INSERT` que
hiciera Postgres por su cuenta chocaría con una clave primaria existente.

!!! clave "Cómo se verifica de verdad la idempotencia"
    El job es idempotente porque trunca antes de cargar. La primera comprobación
    que se hizo **no valía nada**: el log decía «349.009 filas cargadas» en las
    dos ejecuciones, pero eso solo contaba lo que el job escribía. Si el
    `TRUNCATE` hubiera fallado, el log habría sido idéntico y la tabla tendría el
    doble.

    La comprobación válida es **leer los conteos del RDS después**. Desde
    entonces, todas las verificaciones de este proyecto se hacen leyendo el
    estado final del sistema, no el registro del proceso.

---

# 8. Capa Bronze: ingesta incremental

```bash
make bronze                   # todas las tablas
make bronze TABLES=orders     # solo algunas
make watermarks               # hasta donde llego cada una
```

Escribe en `s3://<bucket>/bronze/ecommerce/<tabla>/ingestion_date=YYYY-MM-DD/`.

**Regla de oro: Bronze no limpia nada.** Ni deduplica, ni castea, ni descarta
filas malas. Si mañana cambia una regla de negocio, Silver se reprocesa desde
aquí sin volver a tocar producción.

Lo único que se añade son tres columnas de linaje: `_ingested_at`,
`_source_system` y `_batch_id` — el id del run de Glue, que permite rastrear
cualquier fila hasta la ejecución que la trajo.

## 8.1 Cómo funciona el incremental

Cada tabla guarda en SSM Parameter Store hasta qué `updated_at` llegó. La
siguiente ejecución solo pide lo posterior, con el filtro empujado a Postgres
(no se trae la tabla para filtrar después).

Dos decisiones que evitan perder filas en silencio:

- **El watermark avanza hasta el último `updated_at` leído de verdad**, no hasta
  «ahora». Con la hora actual, cualquier fila confirmada en el origen *mientras*
  el job leía quedaría fuera para siempre.
- **Se escribe solo al terminar bien.** Si el job falla después de escribir en
  S3, la siguiente ejecución repite filas. Repetir es inofensivo —Bronze es
  append-only y Silver deduplica—; perder no tiene arreglo.

`order_items` se lee con 4 particiones JDBC en paralelo, para no saturar el RDS
con un solo hilo.

## 8.2 Verificado en AWS

| Prueba | Resultado |
|---|---|
| Carga inicial (watermark en el epoch) | 349.009 filas |
| Reejecución sin cambios | **0 filas** |
| Tras un día de actividad simulada | **16.060 filas** — 4,5% de la tabla |

Ese «0 filas» de la segunda ejecución es toda la prueba de que el incremental
funciona.

## 8.3 Una decisión que quedó abierta

Bronze **no está registrada en el Glue Data Catalog**: es Parquet suelto, así
que consultarla desde Athena exige declarar la tabla a mano. Se evaluaron cuatro
opciones (crawler, declararla en el CDK, `saveAsTable`, o convertirla también a
Iceberg) y ninguna se implementó. La más limpia sería hacer de `config.py` la
única fuente de verdad del esquema y generar la declaración desde ahí.

Queda anotado como deuda consciente, que es distinto de un olvido.

---

# 9. Capa Silver: limpieza, cuarentena y MERGE

## 9.1 El orden importa

1. **Normaliza** (`trim`, `lower`, `upper`). Va **antes** de validar, para no
   mandar a cuarentena una fila cuyo único problema era un espacio sobrante. Lo
   que no se arregla adivinando: un `ESP` no se convierte en `ES`.
2. **Deduplica** por clave de negocio con `row_number()`, quedándose con la más
   reciente. Desempata por `_ingested_at`, sin lo cual el resultado dependería
   del orden en que Spark leyera los ficheros —es decir, no sería determinista.
3. **Valida** contra las reglas de `config.py`.
4. **`MERGE INTO`** la tabla Iceberg, que es lo que hace la capa idempotente.

## 9.2 Cuarentena, no descarte

Las filas que fallan van a `silver/_quarantine/<tabla>/` con `_quality_errors`:
un array con **todos** sus problemas, no solo el primero. Si un pedido tiene el
importe negativo *y* el cliente huérfano, uno se entera de las dos cosas a la
vez en lugar de arreglar una y descubrir la otra mañana.

## 9.3 El efecto cascada, y cómo se corrigió

Este es el hallazgo más interesante de la fase.

La primera versión validaba la integridad referencial contra la tabla Silver ya
fusionada, es decir, contra **las filas que habían sobrevivido a la limpieza**.
Medido con los datos de prueba:

```
   628 clientes a cuarentena (email nulo, pais mal formado)
         ↓ dejaban huerfanos
 3.253 pedidos  (ademas de los 2.541 con problemas propios)
         ↓ dejaban huerfanas
19.303 lineas de pedido
```

Un 1% de huérfanos reales se convertía en un **10%** de cuarentena. Casi 7× de
amplificación, y la tasa dejaba de significar nada.

El arreglo **no toca Bronze** —sigue siendo una copia tonta e inmutable—. Lo que
cambia es contra qué se compara: el universo de claves **vistas en el origen**,
todo el lote incluidas las filas que van a cuarentena, unido al histórico ya en
Silver. Una fila cuyo padre existe pero está en cuarentena no es un dato malo.

| | Antes | Ahora |
|---|---|---|
| `order_items` en cuarentena | 9,95% | **2,03%** |
| Total | 8,76% (el gate saltaba) | **2,55%** |

La contrapartida hay que conocerla: **Silver ya no es referencialmente
cerrada**. Puede haber un pedido cuyo cliente no esté en `silver.customers`. Ese
problema lo hereda Gold, y lo resuelve con el miembro desconocido (cap. 10).

## 9.4 Umbrales por tabla

Cada tabla declara **su propio umbral**, porque no significan lo mismo: un 3% de
clientes con el correo mal escrito es ruido normal de un formulario web; un 3%
de productos con precio negativo es un incidente.

| Tabla | Umbral | Medido |
|---|---|---|
| `customers` | 6% | 3,02% |
| `products` | 3% | 1,45% |
| `orders` | 5% | 3,99% |
| `order_items` | 5% | 2,03% |

El job **no decide** si el pipeline sigue: escribe su informe en
`s3://<bucket>/_quality/silver/latest.json` y quien decide es la máquina de
estados (cap. 11). Esa separación llegó en la Fase 7 y se comenta allí.

Nótese qué significa exactamente un corte: el `MERGE` ya ocurrió y solo escribió
filas válidas, así que Silver no queda corrupta. Lo que se impide es que **Gold
se construya sobre un lote del que se ha rechazado demasiado**. No es «hay datos
malos publicados», es «se ha perdido tanto que los agregados no serían
representativos».

---

# 10. Capa Gold: el modelo estrella

| Tabla | Qué es |
|---|---|
| `dim_date` | Calendario generado, clave `AAAAMMDD` |
| `dim_product` | SCD tipo 1: estado actual, sin historia |
| `dim_customer` | SCD tipo 2: una versión por cada cambio |
| `fct_order_items` | Los hechos, a grano de **línea de pedido** |
| `agg_daily_sales` | Ingresos, unidades y ticket medio por día y categoría |

## 10.1 El grano

`fct_order_items` tiene grano de **línea**, no de pedido: es el más atómico
disponible, y desde ahí se puede agregar a cualquier nivel.

Consecuencia que hay que tener presente: **no lleva `total_amount`** del pedido.
Repetirlo en cada línea lo duplicaría al sumar. El total de un pedido se obtiene
sumando sus líneas.

## 10.2 SCD tipo 2 en `dim_customer`

Cuando un cliente cambia de segmento no se sobrescribe: se cierra la versión
anterior y se abre otra.

```
customer_id  segment   valid_from            valid_to              is_current
          7  silver    1970-01-01 00:00:00   2026-08-06 15:53:55   false
          7  platinum  2026-08-06 15:53:55   9999-12-31 00:00:00   true
```

Detalles que no son obvios:

- **La historia no se puede reconstruir.** Silver solo guarda el estado actual
  (el `MERGE` sobrescribe), así que el SCD2 se construye ejecución a ejecución.
  La historia empieza el día que empiezas a ejecutar Gold.
- **No todos los cambios abren versión.** Solo `segment`, `country_code` y
  `marketing_opt_in`.
- **`valid_to` usa `9999-12-31`, no `NULL`**, para que el *as-of join* se
  escriba sin un `OR valid_to IS NULL`.
- Los atributos se comparan con `<=>` (null-safe) y no con `!=`.

Verificado en AWS: **1.103 versiones nuevas abiertas y 709 cerradas**, con todas
las invariantes en pie (ningún cliente con dos versiones vigentes, ningún hueco
temporal).

## 10.3 Claves subrogadas deterministas

Se calculan con `xxhash64` de la clave de negocio, más `valid_from` en las
dimensiones con historia. **No** con un contador: con
`monotonically_increasing_id`, cada reconstrucción de Gold reasignaría claves y
los hechos ya escritos pasarían a apuntar a la fila equivocada, sin que nada
fallara.

## 10.4 El miembro desconocido

La fila `-1` de cada dimensión, adonde van los hechos huérfanos. Resuelve la
herencia del capítulo anterior. Medido en AWS con datos reales:

| segment | líneas | ingresos |
|---|---|---|
| platinum | 68.832 | 93.451.359,99 |
| gold | 56.105 | 75.856.346,35 |
| bronze | 55.620 | 75.138.173,21 |
| silver | 55.506 | 75.059.365,51 |
| **(desconocido)** | **17.156** | **23.407.988,81** |

Esos 23,4 millones son un **7% de los ingresos** que un `INNER JOIN` habría
perdido en silencio. Con el miembro desconocido siguen contando y el problema
queda visible y perseguible.

## 10.5 El cuadre

El job **falla si los ingresos de Gold no coinciden con los de Silver**. Si un
join pierde o duplica líneas, se sabe ahí y no cuando el negocio se queje.
Verificado: **342.913.233,87** exacto en las dos capas.

!!! aviso "Una trampa de Iceberg"
    Particionar con `F.expr("months(order_date)")` falla con
    `Invalid partition transformation`: una expresión genérica de Spark no se
    reconoce como transformación de partición. Hay que usar las funciones
    dedicadas (`F.months`, `F.days`…).

---

# 11. Orquestación con Step Functions

```
Bronze ──► Silver ──► LeerInformeDeCalidad ──► ¿pasa? ──no──► SNS ──► Fail
                                                  │ si
                                                  ▼
                                                Gold ──► Success
```

## 11.1 Tres decisiones que se apartan del plan

**No hay estado `Map` sobre las tablas.** El plan proponía un `Map` con
concurrencia 3 que lanzara un job por tabla. No compensa: `bronze_ingest` ya
recorre las cuatro tablas en una sola ejecución y Spark paraleliza por dentro.
Un `Map` serían cuatro arranques de Glue —cerca de un minuto cada uno solo de
arrancar— en lugar de uno, más caro y más lento a cambio de nada.

**La puerta de calidad la aplica la máquina, no el job.** El job de Silver
escribe su informe y no decide. Así el criterio vive en un solo sitio y solo hay
un sitio que lo aplique; y permite distinguir dos cosas que no son iguales: que
el pipeline se pare porque los datos venían mal (flujo controlado, con su
mensaje) y que se pare porque un job reventó (el `Catch`).

**El informe se lee sin Lambda.** La integración SDK de Step Functions llama a
`s3:getObject` directamente. Una Lambda solo para leer un JSON sería una pieza
más que mantener, desplegar y vigilar.

## 11.2 Detalles que solo fallan en ejecución

La integración es `.sync` (`RUN_JOB`). Con `REQUEST_RESPONSE`, la máquina
seguiría al estado siguiente nada más lanzar el job y Silver empezaría con
Bronze a medias — **sin dar ningún error**.

!!! clave "Un bug encontrado antes de desplegar"
    SNS exige que `Message` sea un **string**. Pasar `$.error` a secas manda un
    objeto y la publicación falla **en ejecución, no en el `synth`**: es de los
    errores que cuestan un despliegue y diez minutos de espera.

    Se encontró inspeccionando la plantilla generada antes de desplegar, y ahora
    hay un test que lo fija. La moraleja de la fase: la plantilla es un artefacto
    que se puede leer, y leerla es más barato que desplegar.

## 11.3 Verificado en AWS

| Prueba | Resultado |
|---|---|
| Pipeline completo | `SUCCEEDED` en **7 min 4 s** |
| Recorrido | Bronze → Silver → LeerInforme → Choice → Gold → Success |
| Con un lote corrupto (`--dirt-factor 20`) | `FAILED` con error `CalidadInsuficiente` |
| Recorrido en ese caso | Bronze → Silver → LeerInforme → Choice → **AvisarCalidad** → Fail |
| ¿Se construyó Gold? | **No.** No aparece en el histórico de ejecución. |

La causa del corte fue `orders` al 7,69% frente a su umbral del 5%.

## 11.4 ¿Fue buena idea dejar la orquestación para el final?

No, y conviene decirlo. La práctica habitual es un *walking skeleton*: un
recorrido fino pero completo desde el primer día. Dejarla para el final costó
tres cosas concretas:

1. El `run_glue_job` del `Makefile` era un orquestador artesanal que ahora
   duplica lo que hace Step Functions.
2. La puerta de calidad tuvo que **salir** del job de Silver, donde se había
   puesto dos fases antes.
3. Los jobs no devuelven datos al orquestador, así que el informe de calidad
   viaja por S3 en lugar de por el estado de la máquina.

---

# 12. CI/CD

Tres workflows en `.github/workflows/`:

| Workflow | Cuándo | Qué hace |
|---|---|---|
| `ci.yml` | cada PR y push a ramas protegidas | lint, 104 tests unitarios, 65 de infraestructura, `cdk synth` |
| `deploy-dev.yml` | push a `develop` / manual | `cdk diff` siempre; despliega solo si se pide |
| `deploy-prod.yml` | push a `main` / manual | igual, más aprobación manual |

## 12.1 El CI no puede quedarse verde mintiendo

Este es el punto central de la fase, y no tiene nada que ver con YAML.

`tests/conftest.py` y seis ficheros de test usan
`pytest.importorskip("pyspark")`. En un portátil es una comodidad. En un runner
sin el contenedor de Glue es una mentira: un *skip de colección* no cambia el
código de salida, así que pytest termina en 0, el check sale verde, y se han
ejecutado **20 de los 97 tests**.

La solución tiene dos capas, activadas por `PRACTICA_EXIGE_PYSPARK=1`:

1. **`pytest_configure`**, que falla **antes de recolectar**. Es la capa que
   atrapa el caso real: los `importorskip` de módulo se disparan al importar, o
   sea antes de que exista ningún test al que marcar como fallido.
2. El hook **`pytest_runtest_makereport`**, que convierte cualquier salto
   individual en fallo.

Y se comprobó provocándolo, que es lo único que demuestra que funciona:

```bash
python3 -m pytest tests/unit -m "not integration"      # exit 0, saltando tests
PRACTICA_EXIGE_PYSPARK=1 python3 -m pytest tests/unit  # exit 4, con el motivo
make test-ci                                           # 104 passed
```

Los tests corren dentro de `public.ecr.aws/glue/aws-glue-libs:5`. Las
alternativas se descartaron con motivo: instalar PySpark en el runner no vale
porque `src/jobs/*.py` importan `awsglue.utils`, que no está en PyPI, y porque
los JAR vendrían de Maven en vez de ser los de Glue 5.0 — si divergen, el CI
diría verde y el job real fallaría.

## 12.2 OIDC: sin claves guardadas

`infra/stacks/cicd_stack.py` crea el proveedor OIDC y dos roles. No hay ninguna
clave de acceso en los secretos del repositorio.

**La condición sobre el claim `sub` es la que lo sostiene todo.** El emisor y el
`aud` son idénticos para todos los repositorios de GitHub: sin esa condición,
cualquiera podría crear un repositorio, copiar el ARN del rol —que está escrito
en el workflow, y **este repo es público**— y entrar en la cuenta. En CloudTrail
se vería una autenticación perfectamente legítima.

**El rol de prod confía en `environment:prod`, no en `refs/heads/main`.** Ese
claim solo aparece si el job declara el entorno, que es lo único que dispara el
revisor obligatorio. Aceptar también la rama dejaría la puerta pintada en la
interfaz sin cerrar nada. Así el diseño **falla cerrado**: olvidarse de declarar
el entorno no salta la aprobación, impide desplegar.

**El rol no despliega nada por sí mismo**: solo asume los roles del bootstrap
del CDK. `AdministratorAccess` funcionaría igual y convertiría un push malicioso
a `develop` en control total de la cuenta.

Los 14 tests de `infra/tests/test_cicd.py` fijan exactamente esos cuatro fallos.
Ninguno de ellos da error al desplegar.

## 12.3 El stack que no se puede borrar

`Practica-Cicd` es el huevo y la gallina: hay que desplegarlo a mano, porque
hasta que exista no hay rol que permita desplegar desde Actions. Y **no debe
desaparecer** con el `make destroy-dev` del final de cada sesión.

Se resuelve construyéndolo solo con `-c cicd=true`, de modo que queda fuera del
`--all`. No es cosmética: `destroy --all` actúa sobre los stacks que `app.py`
construye; si no se construye, no existe para él. Confiar en un `--exclusively`
dependería de acordarse justo el día que haya prisa.

## 12.4 El interruptor de coste

Al mergear en `develop` solo se ejecuta `cdk diff`: lee AWS y cuenta qué
cambiaría, sin crear nada. El despliegue real se pide a mano.

```bash
gh variable set DESPLIEGUE_AUTOMATICO_DEV --body true   # para pasar a CD continuo
```

Es una variable de repositorio y no un cambio en el YAML a propósito: se activa
o se revierte en segundos, sin PR y sin protección de rama de por medio. Esta
infraestructura cuesta ~0,05 USD/hora, y un merge no debería poder resucitarla
sin que se pida.

## 12.5 Por qué `cdk synth` está en el CI

Los 65 tests de infraestructura importan los stacks y construyen su propio
`cdk.App`: **nadie ejecuta `infra/app.py`**. Un parámetro renombrado en el
cableado o una dependencia entre stacks mal puesta pasarían los 65 tests y
romperían el primer despliegue.

El `synth` se hace **sin credenciales**: al no exportar `CDK_DEFAULT_ACCOUNT`,
el stack queda agnóstico y la VPC usa `Fn::GetAZs` en vez de disparar el context
provider `availability-zones`, que llamaría a EC2 de verdad. El synth vale
precisamente porque no sabe en qué cuenta está.

!!! aviso "Dos trampas de GitHub que cuestan una tarde"
    Marcar un check como obligatorio **antes** de fusionar el workflow que lo
    publica deja todos los PR esperando eternamente un resultado que nadie va a
    reportar. Primero se fusiona `ci.yml`, después se exige.

    Y GitHub **no registra los workflows hasta que llegan a la rama por
    defecto**: en un repositorio que nunca ha usado Actions, el PR que introduce
    el CI no dispara su propio CI.

## 12.6 Lo que quedó sin verificar, y cómo se distingue

El día que se montó esta fase, **GitHub Actions estaba en caída mayor**. Los
workflows se registraron correctamente, pero ningún runner llegó a asignarse: el
job estuvo quince minutos encolado y GitHub lo canceló con **cero pasos
ejecutados**.

Merece la pena contar cómo se llegó a esa conclusión, porque el síntoma —«el CI
no arranca»— apunta por defecto a que uno ha escrito mal el YAML. Se descartó en
este orden: el YAML parsea; el token tiene scope `workflow`; el repositorio es
público y no es un fork; Actions está habilitado; los tres workflows aparecen
como `active`; y un `workflow_dispatch` **sí** crea el run, que se queda
encolado. Lo último es lo que descarta la configuración y señala a la
infraestructura, y se confirmó en `githubstatus.com`.

Así que del CI está verificado todo menos su ejecución:

| Verificado | Cómo |
|---|---|
| El rol OIDC y su trust policy | `aws iam get-role`, sobre el rol ya desplegado |
| El mecanismo anti-skip | provocándolo: exit 0 saltando tests vs exit 4 |
| `cdk synth` sin credenciales | exit 0 |
| `Practica-Cicd` fuera del `--all` | `cdk list` con y sin `-c cicd=true` |
| **Un run del workflow** | **pendiente** |

Decirlo así, y no «el CI funciona», es la diferencia entre documentación y
propaganda.

---

# 13. Calidad: tests, linting y pre-commit

Dos suites, con dos entornos distintos:

| Suite | Dónde corre | Cuántos |
|---|---|---|
| `tests/unit` | contenedor de Glue | **104** |
| `infra/tests` | venv del host | **65** |

Los tests de infraestructura no tocan AWS: construyen la plantilla en memoria
con `Template.from_stack` y hacen aserciones sobre ella. Cada uno lleva un
docstring que dice **qué error real previene**, no qué comprueba — la diferencia
importa, porque un test cuyo motivo no está escrito es un test que alguien
borrará cuando estorbe.

`ruff` vive dentro del contenedor, no en el host, para que todo el mundo use la
misma versión sin instalar nada. Y el CI ejecuta `pre-commit run --all-files` en
lugar de `ruff` suelto, para que la versión salga del `.pre-commit-config.yaml` y
no haya dos fuentes de verdad.

---

# 14. Referencia del Makefile

**Entorno**

| Target | Qué hace |
|---|---|
| `up` / `down` / `clean` | levanta, para, o para y borra el volumen |
| `shell` | bash dentro del contenedor de Glue |
| `psql` | consola SQL contra el Postgres local |

**Datos**

| Target | Qué hace |
|---|---|
| `schema` / `seed` / `seed-daily` | DDL, carga inicial, simular un día |
| `export-seed` / `seed-rds` | el rodeo por S3 hasta el RDS |

**Calidad**

| Target | Qué hace |
|---|---|
| `test` / `test-ci` | tests unitarios; `test-ci` exige PySpark en vez de saltarlo |
| `test-infra` | los 65 tests de la plantilla |
| `lint` / `format` | ruff |

**Despliegue**

| Target | Qué hace |
|---|---|
| `synth` / `diff` | plantilla y diferencias, sin tocar nada |
| `deploy-dev` / `deploy-prod` / `destroy-dev` | despliegue y borrado |
| `deploy-cicd` / `diff-cicd` | el stack de OIDC, aparte del resto |

**Pipeline**

| Target | Qué hace |
|---|---|
| `bronze` / `silver` / `gold` / `pipeline` | los jobs, uno a uno o encadenados |
| `run` / `history` | la máquina de estados |
| `quality` / `watermarks` / `reset-watermarks` | inspección del estado |

**Documentación**

| Target | Qué hace |
|---|---|
| `docs` | genera los dos PDF desde los Markdown de `docs/` |

---

# 15. Costes

| Recurso | Coste si se deja desplegado |
|---|---|
| VPC, subredes, security groups | gratis |
| Endpoint **Gateway** de S3 | gratis |
| Endpoints de **interfaz** | ~0,01 USD/h **por AZ** cada uno → ~29 USD/mes en 1 AZ |
| NAT Gateway | ~33 USD/mes — **no se usa** |
| RDS `db.t4g.micro`, 20 GB | gratis los primeros 12 meses; después ~13 USD/mes |
| S3, Step Functions, Athena | céntimos a este volumen |
| Glue 5.0 | 0,44 USD/DPU-hora, mínimo 2 DPU → ~0,05-0,15 USD por ejecución |
| Stack de CI/CD (solo IAM) | **gratis** |

Lo anterior solo importa si se deja la infraestructura levantada. La forma
correcta de usar este laboratorio es destruirla al acabar, y entonces unas horas
sueltas cuestan céntimos.

---

# 16. Git Flow, la release y el hotfix

## 16.1 Las ramas

Cada fase del proyecto ha sido una rama `feature/*` con su PR contra `develop`:
ocho fases, ocho PR. `main` estuvo en el commit inicial hasta la versión 1.0.0,
que fue **la primera fusión real hacia producción**.

## 16.2 La release 1.0.0

La rama `release/1.0.0` solo toca tres cosas: el número de versión en
`pyproject.toml` (único sitio donde vive), el `CHANGELOG.md` y el estado del
README.

Tres detalles de procedimiento que se olvidan y cuestan caro:

- **Los dos PR se crean antes de fusionar ninguno**, y el primer merge va **sin
  `--delete-branch`**. Si la rama desaparece, el PR de vuelta a `develop` se
  cierra solo y `develop` se queda sin el bump ni el changelog.
- **Merge commit, nunca squash.** Un squash crea un commit nuevo sin relación
  con los originales: `main` dejaría de compartir historia con `develop` y el
  back-merge se convertiría en un conflicto de los ocho merges enteros.
- **El tag no viaja con `git push`**: hay que empujarlo explícitamente.

## 16.3 El ejercicio del hotfix

La versión 1.0.0 salió con un bug deliberado, introducido como si fuera una
micro-optimización de última hora — que es exactamente cómo entran estos bugs de
verdad. En `agg_daily_sales`, el ticket medio pasó a dividirse entre el número
de **líneas** en vez de entre pedidos distintos:

```python
.withColumn("avg_ticket", F.round(F.col("revenue") / F.col("lines"), 2))
```

Es creíble por cuatro razones, y las cuatro son la lección:

1. El mensaje del commit era **técnicamente cierto** en su premisa
   (`countDistinct` sí cuesta un shuffle) y falso solo en la conclusión.
2. **El esquema no cambia**, así que ningún test de esquema salta.
3. **No existía ningún test sobre `gold_build.py`.** La suite entera seguía en
   verde. Esa ausencia es la causa raíz del incidente.
4. Se borró el comentario que avisaba justo de este error: el patrón clásico de
   «el comentario estorbaba».

El bug **no da ningún error**. El job termina `SUCCEEDED`, el cuadre de ingresos
con Silver pasa —porque comprueba `revenue`, no `avg_ticket`— y la puerta de
calidad pasa, porque mide Silver y no Gold. Las tablas se escriben y el número
está mal.

Se detecta cuadrando en Athena: si `avg_ticket` es el ticket medio por pedido,
`avg_ticket * orders` tiene que aproximar `revenue`. Medido en AWS con el bug
desplegado:

| category | revenue | orders | lines | avg_ticket | reconstruido |
|---|---|---|---|---|---|
| hogar | 1.128.091,55 | 719 | 815 | 1.384,16 | **995.211,04** |
| deporte | 1.380.179,85 | 831 | 967 | 1.427,28 | **1.186.069,68** |

Un 12% de desviación, exactamente la proporción `orders/lines`. Y tras el
arreglo, el mismo día y la misma categoría: `avg_ticket` 1.568,97 y
`reconstruido` 1.128.089,43, que cuadra con el ingreso real salvo el redondeo a
dos decimales.

`hotfix/1.0.1` sale de `main`, arregla el cálculo, **añade el test de regresión**
y vuelve a `main` **y a `develop`**.

Que el test caza el bug se comprobó reintroduciendo la división mala: tres tests
fallan con `assert 100.0 == 200.0`. Un test que no se ha visto fallar no es un
test.

Esa doble fusión no es burocracia, y aquí se ve por qué mejor que en ningún otro
ejemplo: **el bug entró por la rama de release, que se fusionó a las dos ramas**,
así que `develop` también lo tiene. Arreglando solo `main`, la siguiente versión
que saliera de `develop` reintroduciría el bug —y con un tag posterior, de modo
que parecería una regresión nueva y nadie miraría el hotfix.

El test de regresión exigió un refactor pequeño: extraer la agregación pura del
envoltorio que lee y escribe en el catálogo. Un cálculo que no se puede probar en
tres líneas es un cálculo que se volverá a romper.

---

# 17. Bitácora de problemas

Todos los fallos reales del proyecto, con su síntoma y su causa. Los modos de
fallo genéricos que ilustran están explicados en el glosario.

## 17.1 Entorno local

| Síntoma | Causa y arreglo |
|---|---|
| `permission denied` en `/var/run/docker.sock` | La shell abierta es anterior al `usermod -aG docker`. `newgrp docker` o volver a entrar. |
| El contenedor de Glue sale nada más arrancar | El `ENTRYPOINT` es `bash -l`, así que `command: ["sleep","infinity"]` ejecuta `bash -l sleep infinity`. Usar `["-c", "sleep infinity"]`. |
| El contenedor no puede escribir en el proyecto | uid 10000 del usuario `hadoop` frente al del host. `user: "10000:${HOST_GID}"`. |
| `ruff`/`pytest` fallan al escribir un fichero concreto | Ese fichero se creó en una shell con `newgrp docker` y tiene grupo `docker`. `chgrp`. |
| `ClassNotFoundException` al crear una tabla Iceberg en local | Los JAR están en la imagen pero fuera del classpath. Los añade `spark_session.py` con un glob. |
| El venv del CDK no se crea en Ubuntu | Ubuntu separa `ensurepip`. `sudo apt install python3.12-venv`. |

## 17.2 AWS

| Síntoma | Causa y arreglo |
|---|---|
| Un job de Glue se cuelga y muere por timeout sin mensaje | Falta la regla del security group que se referencia a sí mismo, o falta el endpoint del servicio que intenta alcanzar. |
| `ConnectTimeoutError` tras dos minutos, sin más pistas | Faltaba el VPC endpoint de SSM. Hay un test de regresión que enumera todos los servicios usados. |
| `DELETE_FAILED` al destruir el stack de red | ENIs de Glue huérfanas que todavía no se habían liberado. Borrarlas a mano; y esperar unos minutos tras el último job antes de destruir. |
| Las tablas no aparecen en Athena aunque los datos estén en S3 | `running_on_glue()` no funcionaba: se escribía con el catálogo Hadoop. Ver §5.2. |
| `Invalid partition transformation: months(order_date)` | `F.expr(...)` no se reconoce como transformación de partición. Usar `F.months`. |
| Un valor de secreto que aparece troceado | El ARN de un secreto lleva un sufijo aleatorio; hay que referenciarlo por nombre, no reconstruirlo. |
| Bronze vacío tras un `destroy` + `deploy` | Los watermarks viven en SSM y sobreviven al borrado del stack. `make reset-watermarks`. |
| El coste de los endpoints de interfaz supera al del NAT | Se pagan por endpoint **y por AZ**. Desplegarlos en una sola AZ. |

## 17.3 Procesos y verificación

| Síntoma | Causa y arreglo |
|---|---|
| Una prueba de idempotencia que no probaba nada | Contaba lo que el job escribía, no el estado final. Leer los conteos del sistema. |
| La cuarentena se dispara al 10% sin datos malos | Efecto cascada: se validaba contra las supervivientes. Validar contra el universo del origen. |
| `run_glue_job` esperando diez minutos a un id vacío | Faltaba un `test -n "$RUN"` que comprobara que el job llegó a arrancar. |
| El CI en verde habiendo probado el 20% de la suite | `importorskip` en un entorno sin PySpark. Ver §12.1. |
| El PR que introduce el CI no dispara el CI | GitHub no registra los workflows hasta que llegan a la rama por defecto. |

## 17.4 Lo que se aprendió

Tres cosas se repiten en casi todas las filas de arriba:

1. **Los fallos caros no dan error.** Dan éxito con el resultado mal. El
   catálogo equivocado, el join que multiplica, la media al grano equivocado, el
   CI que se salta los tests: ninguno falló nunca.
2. **La defensa contra eso no es un comentario, es una comprobación.** Y una
   comprobación que no se ha visto fallar no es una comprobación: hay que
   provocarla.
3. **Leer el artefacto antes de desplegarlo es barato.** El bug de SNS se
   encontró leyendo la plantilla, en segundos. Los demás se encontraron
   desplegando, en decenas de minutos cada uno.
