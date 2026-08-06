# practica — Data Pipeline Medallion

Pipeline de datos end-to-end: **Postgres → S3** con arquitectura medallion
(bronze / silver / gold), construido con **AWS Glue 5.0**, **PySpark**,
**Apache Iceberg** y **Step Functions**, desplegado con **AWS CDK** y
desarrollado siguiendo **Git Flow**.

Proyecto de práctica de Data Engineering / DataOps.

---

## Arquitectura

```
RDS Postgres ──JDBC──► Glue: bronze_ingest ──► S3 bronze/  (Parquet crudo, inmutable)
                                                    │
                              Glue: silver_transform ──► S3 silver/ (Iceberg, MERGE)
                                                    │
                                        ¿calidad OK? ──no──► SNS, se detiene
                                                    │ sí
                                    Glue: gold_build ──► S3 gold/ (modelo estrella)
                                                    │
                                              Athena / Glue Data Catalog
```

Todo orquestado por una máquina de estados de **Step Functions** disparada a diario
por EventBridge.

| Capa | Formato | Qué contiene |
|---|---|---|
| **Bronze** | Parquet, particionado por `ingestion_date` | Copia fiel del origen. No se limpia nada. Append-only. |
| **Silver** | Iceberg | Tipado, deduplicado, validado. `MERGE INTO` idempotente. Las filas malas van a cuarentena. |
| **Gold** | Iceberg | Modelo estrella: `dim_customer` (SCD2), `dim_product`, `fct_orders`, `agg_daily_sales`. |

---

## Empezar

### Requisitos

- Docker + Docker Compose
- AWS CLI v2 y Node.js LTS (`npm i -g aws-cdk`) — solo a partir de la Fase 2
- `make`

No necesitas Java ni Spark en tu máquina: todo corre dentro del contenedor
oficial de Glue 5.0 (`public.ecr.aws/glue/aws-glue-libs:5`), que trae el mismo
runtime que AWS. Verificado en la imagen: **Spark 3.5.2, Python 3.11.15,
Iceberg 1.7.1-amzn-1**. (La documentación de AWS anuncia Spark 3.5.4; el tag
`:5` va ligeramente por detrás.)

`local/Dockerfile` extiende esa imagen con lo poco que le falta: `faker`,
`psycopg` y `ruff`.

### Entorno local

```bash
cp .env.example .env      # ajusta si quieres
make up                   # levanta Postgres + contenedor Glue
make seed                 # carga histórica de datos sintéticos
make test                 # tests unitarios (debe pasar el test de humo de Iceberg)
```

Comandos útiles:

```bash
make help                 # lista todos los targets
make shell                # bash dentro del contenedor de Glue
make psql                 # consola SQL contra el Postgres local
make seed-daily           # simula un día nuevo: altas, cambios y datos sucios
make down                 # para todo (conserva los datos)
make clean                # para todo y borra el volumen de Postgres
```

### Datos de prueba

`data_generator/seed.py` genera un e-commerce sintético con **suciedad deliberada**
(emails sin normalizar, duplicados, nulos en campos obligatorios, importes negativos,
claves foráneas huérfanas). Esa suciedad es lo que la capa Silver tiene que detectar.

Para probar que el gate de calidad detiene el pipeline:

```bash
make shell
python3 data_generator/seed.py --mode daily --dirt-factor 10
```

---

## Infraestructura (CDK)

El CDK corre en el **host**, no en el contenedor: necesita Node y tus credenciales
AWS. Los jobs de Spark corren en el contenedor. Son dos mundos separados.

```bash
sudo apt install -y python3.12-venv   # una sola vez: Ubuntu separa ensurepip
make infra-deps                       # crea infra/.venv e instala aws-cdk-lib
make synth                            # genera el CloudFormation sin desplegar
make diff                             # que cambiaria en AWS
make deploy-dev                       # despliega
make destroy-dev                      # destruye (hazlo al acabar)
```

| Stack | Qué crea |
|---|---|
| `Practica-Dev-Network` | VPC 10.20.0.0/16, 2 AZ, solo subredes aisladas (sin NAT ni IGW), security groups de Glue y RDS, VPC endpoints |
| `Practica-Dev-Storage` | Bucket `practica-datalake-dev-<cuenta>-<región>` y las tres bases del Glue Data Catalog |
| `Practica-Dev-Database` | RDS Postgres 16.9 `db.t4g.micro` en subredes aisladas, credenciales en Secrets Manager, y la Glue Connection JDBC |
| `Practica-Dev-Glue` | Rol de ejecución de Glue, publicación de scripts a S3, y los jobs `seed-rds`, `bronze-ingest`, `silver-transform` y `gold-build` |
| `Practica-Dev-Orchestration` | Máquina de estados de Step Functions, topic SNS y regla de EventBridge |

Dos detalles que merecen atención:

- El security group de Glue **se permite a sí mismo todo el tráfico**. No es un
  descuido: los nodos del cluster de Spark se hablan por puertos arbitrarios y
  Glue lo exige. Sin esa regla el job se cuelga y falla por timeout sin decir
  por qué.
- Las bases del catálogo se **declaran**, no se descubren con un Crawler. Un
  crawler cuesta en cada ejecución, tarda, y adivina el esquema.

---

## Sembrar el RDS

El RDS vive en subredes aisladas: **no lo alcanzas con `psql` desde tu portátil**,
y eso es deliberado. Es el mismo problema que tendrías en cualquier empresa que
haya montado la red bien. La ruta es:

```
Postgres local ──export_seed.py──► data/_seed ──aws s3 sync──► S3 ──job de Glue──► RDS
```

```bash
make deploy-dev      # crea el RDS (tarda ~10 min la primera vez)
make up && make seed # datos en el Postgres local, si no los tienes ya
make export-seed     # exporta a Parquet y sube a S3
make seed-rds        # lanza el job de Glue y espera a que termine
make db-creds        # ver usuario y contraseña, si los necesitas
```

`make seed-rds` es idempotente: trunca las tablas antes de cargar, así que
puedes relanzarlo las veces que quieras.

Detalles que merecen atención:

- **La contraseña no existe en el código.** La genera AWS y vive en Secrets
  Manager. En la plantilla de CloudFormation solo aparece como
  `{{resolve:secretsmanager:...}}`, y en los argumentos del job solo va el
  *nombre* del secreto.
- **La Glue Connection es lo que mete el job dentro de la VPC.** Sin ella el
  job corre en la red de AWS y no ve el RDS. Con ella, Glue levanta ENIs en
  nuestra subred.
- El job aplica el mismo `data_generator/schema.sql` que usas en local: se sube
  a S3 en cada despliegue, así que no hay dos copias que puedan divergir.
- Después de cargar, el job **recoloca las secuencias**. Insertamos los IDs
  explícitamente, así que las secuencias seguirían en 1 y el primer `INSERT`
  que hiciera Postgres por su cuenta chocaría con una clave primaria existente.

---

## Capa Bronze: ingesta incremental

```bash
make bronze                      # ingesta todas las tablas
make bronze TABLES=orders        # solo algunas
make watermarks                  # hasta dónde llegó cada tabla
```

Escribe en `s3://<bucket>/bronze/ecommerce/<tabla>/ingestion_date=YYYY-MM-DD/`.

**Regla de oro: Bronze no limpia nada.** Ni deduplica, ni castea, ni descarta
filas malas. Es una copia fiel del origen. Si mañana cambias una regla de
negocio, reprocesas Silver desde aquí sin volver a tocar producción.

Lo único que se añade son columnas de linaje: `_ingested_at`, `_source_system`
y `_batch_id` (el id del run de Glue, que permite rastrear cualquier fila hasta
la ejecución que la trajo).

### Cómo funciona el incremental

Cada tabla guarda en SSM Parameter Store hasta qué `updated_at` llegó la última
vez. La siguiente ejecución solo pide lo posterior.

Dos decisiones que importan:

- **El watermark avanza hasta el último `updated_at` leído de verdad**, no hasta
  "ahora". Usar la hora actual dejaría fuera cualquier fila que se confirmara en
  el origen mientras el job estaba leyendo — filas perdidas en silencio, el peor
  bug posible en un pipeline.
- **Se escribe solo al final.** Si el job falla después de escribir en S3, la
  siguiente ejecución repite esas filas. Repetir es inofensivo (Bronze es
  append-only y Silver deduplica); perder no.

### Reprocesar un día

Retrasa el watermark y vuelve a lanzar:

```bash
aws ssm put-parameter --overwrite --type String \
  --name /practica/dev/watermark/orders \
  --value 2026-01-01T00:00:00+00:00
make bronze TABLES=orders
```

### Verificado en AWS

| Prueba | Resultado |
|---|---|
| Carga inicial (watermark en epoch) | 349.009 filas |
| Reejecución sin cambios | **0 filas** |
| Tras un día de actividad simulada | **16.060 filas** — 4,5% de la tabla |
| Lectura paralela | `order_items` con 4 particiones JDBC |

---

## Capa Silver: limpieza y MERGE sobre Iceberg

```bash
make silver                  # procesa la partición de hoy
make silver FULL=1           # reprocesa todo el histórico de Bronze
make silver TABLES=orders    # solo algunas tablas
make quality                 # último informe de calidad
```

Escribe en tablas **Iceberg** dentro de `glue_catalog.practica_dev_silver`, que
**se registran solas en el Glue Data Catalog** — Silver queda consultable desde
Athena sin trabajo extra.

### El orden importa

1. **Normaliza** (`trim`, `lower`, `upper`). Va *antes* de validar, para no
   mandar a cuarentena una fila cuyo único problema era un espacio sobrante.
   Lo que no se arregla adivinando: un `ESP` no se convierte en `ES`.
2. **Deduplica** por clave de negocio con `row_number()`, quedándose con la
   versión más reciente. Desempata por `_ingested_at`, sin lo cual el resultado
   dependería del orden en que Spark leyera los ficheros.
3. **Valida** contra las reglas declaradas en `src/common/config.py`.
4. **`MERGE INTO`** la tabla Iceberg.

### Cuarentena, no descarte

Las filas que fallan la validación van a `silver/_quarantine/<tabla>/` con
`_quality_errors`: un array con **todos** sus problemas, no solo el primero.
Si un pedido tiene el importe negativo *y* el cliente huérfano, te enteras de
las dos cosas a la vez.

Descartar filas en silencio es la forma más rápida de perder la confianza en un
data lake: los números no cuadran y nadie sabe por qué.

### La integridad referencial se valida contra el origen, no contra Silver

Detalle sutil con consecuencias grandes. Una clave foránea se considera huérfana
solo si apunta a algo que **nunca existió en el origen** — no si apunta a algo
que no sobrevivió a la limpieza.

La primera versión comparaba contra la tabla Silver ya fusionada, y provocaba un
efecto dominó medido con los datos de prueba:

```
   628 clientes a cuarentena (email nulo, país mal formado)
         ↓ dejaban huérfanos
 3.253 pedidos  (además de los 2.541 con problemas propios)
         ↓ dejaban huérfanas
19.303 líneas de pedido
```

Un 1% de huérfanos reales se convertía en un **10%** de cuarentena. Casi 7× de
amplificación, y la tasa dejaba de significar nada.

El arreglo **no toca Bronze** — Bronze sigue siendo una copia tonta e inmutable.
Lo que cambia es contra qué se compara: el universo de claves vistas en el
origen (todo el lote, incluidas las filas que van a cuarentena) unido al
histórico ya en Silver.

| | Antes | Ahora |
|---|---|---|
| `order_items` en cuarentena | 9,95% | **2,03%** |
| Total | 8,76% (el gate saltaba) | **2,55%** |

La contrapartida, que hay que conocer: **Silver ya no es referencialmente
cerrada**. Puede haber un pedido cuyo `customer_id` no esté en
`silver.customers` porque ese cliente está en cuarentena. Gold lo resolverá con
un `LEFT JOIN` contra una dimensión "desconocido", que es exactamente lo que se
hace en un modelo estrella real.

### El gate de calidad

Cada tabla declara **su propio umbral** en `TableSpec`, porque no significan lo
mismo: un 3% de clientes con el email mal escrito es ruido normal de un
formulario web; un 3% de productos con precio negativo es un incidente.

| Tabla | Umbral | Medido |
|---|---|---|
| `customers` | 6% | 3,02% |
| `products` | 3% | 1,45% |
| `orders` | 5% | 3,99% |
| `order_items` | 5% | 2,03% |

Si alguna lo supera, el job **falla**. Ojo con qué significa eso exactamente:
el `MERGE` ocurre antes de la comprobación y solo escribe filas válidas, así que
Silver no queda corrupta. Lo que impide el fallo es que **Gold se construya
sobre un lote del que se ha rechazado demasiado**: no es "hay datos malos
publicados", es "se ha perdido tanto que los agregados no serían
representativos".

El resultado se deja además en `s3://<bucket>/_quality/silver/latest.json`, que
es lo que leerá la máquina de estados de la Fase 7.

Para probarlo, ensucia un lote a propósito:

```bash
make shell
python3 data_generator/seed.py --mode daily --dirt-factor 10
```

### Reglas declarativas

Añadir una tabla al pipeline es añadir una entrada a `TABLES`, sin tocar código
de Spark:

```python
"customers": TableSpec(
    business_key=["customer_id"],
    lower_trim=["email"],
    upper_trim=["country_code"],
    not_null=["customer_id", "email"],
    patterns={"country_code": r"^[A-Z]{2}$"},
    quarantine_threshold=0.06,
),
```

### Consultar Silver desde Athena

Las tablas Iceberg **se registran solas en el Glue Data Catalog**, así que no
hace falta declararlas ni pasar un crawler:

```sql
SELECT count(*) FROM practica_dev_silver.orders;
SELECT * FROM practica_dev_silver.customers LIMIT 10;
```

---

## Capa Gold: modelo estrella

```bash
make gold        # construye el modelo estrella
make pipeline    # bronze -> silver -> gold de una tacada
```

| Tabla | Qué es |
|---|---|
| `dim_date` | Calendario generado, clave `AAAAMMDD` |
| `dim_product` | **SCD tipo 1**: refleja el estado actual, sin historia |
| `dim_customer` | **SCD tipo 2**: una versión por cada cambio |
| `fct_order_items` | Los hechos, a grano de **línea de pedido** |
| `agg_daily_sales` | Ingresos, unidades y ticket medio por día y categoría |

Los conceptos están explicados en [docs/glosario.md](docs/glosario.md).

### El grano

`fct_order_items` tiene grano de **línea**, no de pedido. Es el más atómico
disponible, y desde ahí se puede agregar a cualquier nivel — al revés no.

Consecuencia que hay que tener presente: **no lleva `total_amount`** del pedido.
Repetirlo en cada línea lo duplicaría al sumar. El total de un pedido se obtiene
sumando sus líneas.

Por lo mismo, el ticket medio de `agg_daily_sales` divide entre **pedidos
distintos**, no entre líneas. Dividir entre líneas daría el importe medio por
línea, que es otra cosa y siempre sale más bajo.

### SCD tipo 2 en `dim_customer`

Cuando un cliente cambia de segmento no se sobrescribe: se cierra la versión
anterior y se abre una nueva.

```
customer_id  segment   valid_from                 valid_to                   is_current
          7  silver    1970-01-01 00:00:00        2026-08-06 15:53:55        false
          7  platinum  2026-08-06 15:53:55        9999-12-31 00:00:00        true
```

Así una consulta sobre marzo usa el segmento que el cliente tenía **en marzo**.
El `as-of join` de `src/common/dimensions.py` lo resuelve:

```sql
ON  f.customer_id = d.customer_id
AND f.order_date >= d.valid_from
AND f.order_date <  d.valid_to
```

Detalles que no son obvios:

- **La historia no se puede reconstruir.** Silver solo guarda el estado actual
  (el `MERGE` sobrescribe), así que el SCD2 se construye ejecución a ejecución.
  La historia empieza el día que empiezas a ejecutar Gold.
- **No todos los cambios abren versión.** Solo `segment`, `country_code` y
  `marketing_opt_in`. Corregir una errata en el nombre no cambia ningún análisis.
- **`valid_to` usa `9999-12-31`, no `NULL`**, para que el join se escriba sin un
  `OR valid_to IS NULL`.
- Los atributos se comparan con `<=>` (null-safe) y no con `!=`. En SQL
  `NULL != 'gold'` es NULL, así que un cambio desde NULL pasaría desapercibido.

### Claves subrogadas deterministas

Se calculan con `xxhash64` de la clave de negocio (más `valid_from` en las
dimensiones históricas), no con un contador. Con `monotonically_increasing_id`,
cada reconstrucción de Gold reasignaría claves y los hechos ya escritos
apuntarían a la fila equivocada.

### El miembro desconocido

La fila `-1` de cada dimensión, adonde van los hechos huérfanos. **No es un
adorno**: resuelve la herencia de la Fase 5, donde Silver deliberadamente no es
referencialmente cerrada.

Medido en AWS con datos reales:

```sql
SELECT d.segment, count(*) lineas, round(sum(f.line_amount),2) ingresos
FROM practica_dev_gold.fct_order_items f
JOIN practica_dev_gold.dim_customer d ON f.customer_key = d.customer_key
GROUP BY d.segment ORDER BY 3 DESC;
```

| segment | líneas | ingresos |
|---|---|---|
| platinum | 68.832 | 93.451.359,99 |
| gold | 56.105 | 75.856.346,35 |
| bronze | 55.620 | 75.138.173,21 |
| silver | 55.506 | 75.059.365,51 |
| **(desconocido)** | **17.156** | **23.407.988,81** |

Esos 23,4 millones son un **7% de los ingresos** que con un `INNER JOIN` se
habrían perdido en silencio. Con el miembro desconocido siguen contando y además
el problema queda visible y perseguible.

### El cuadre

El job **falla si los ingresos de Gold no coinciden con los de Silver**. Si un
join pierde o duplica líneas, se sabe ahí y no cuando el negocio se queje.

---

## Orquestación

```bash
make run       # ejecuta el pipeline completo y espera
make history   # últimas ejecuciones
```

```
Bronze ──► Silver ──► LeerInformeDeCalidad ──► ¿pasa? ──no──► SNS ──► Fail
                                                  │ sí
                                                  ▼
                                                Gold ──► Success
```

Con alertas por correo:

```bash
cdk deploy --all -c environment=dev -c alert_email=tu@correo.com
```

AWS envía un correo de confirmación; hasta que no lo aceptes no llega ninguna
alerta.

### Dos decisiones que se apartan del plan

**No hay estado `Map` sobre las tablas.** El plan proponía un `Map` con
concurrencia 3 lanzando un job por tabla. No compensa: `bronze-ingest` ya
recorre las cuatro tablas en una sola ejecución y Spark paraleliza por dentro.
Un `Map` serían **cuatro arranques de Glue** (~1 minuto cada uno solo de
arrancar) en lugar de uno, más caro y más lento, a cambio de nada. `Map` tiene
sentido cuando cada elemento necesita su propio cluster o cuando quieres que el
fallo de uno no bloquee a los demás.

**La puerta de calidad la aplica la máquina, no el job.** El job de Silver
escribe su informe en S3 y no decide. Así el criterio vive en un solo sitio
(`config.py`) y solo hay un sitio que lo aplica. Además permite distinguir dos
cosas que no son iguales:

| | Significa | Cómo se maneja |
|---|---|---|
| `CalidadInsuficiente` | El pipeline funciona; los datos venían mal | `Choice` → SNS → `Fail` |
| `PipelineFallido` | Algo se rompió | `Catch` → SNS → `Fail` |

Mezclarlas hace imposible saber qué está pasando mirando las alertas.

### Sin Lambda

El informe se lee con la integración SDK de Step Functions
(`aws-sdk:s3:getObject`) y se parsea con `States.StringToJson`. Una Lambda solo
para leer un JSON sería una pieza más que mantener, desplegar y vigilar.

### Detalles que solo fallan en ejecución

- **`.sync` en las tareas de Glue.** Es lo que hace que la máquina *espere*. Con
  `REQUEST_RESPONSE` seguiría al estado siguiente nada más lanzar el job, y
  Silver empezaría con Bronze a medias.
- **SNS exige que `Message` sea un string.** Pasar `$.error` a secas manda un
  objeto y la publicación falla — en ejecución, no en el `synth`. Se envuelve en
  `States.JsonToString(...)`.
- **La regla de EventBridge está deshabilitada en dev.** Que se despierte de
  madrugada contra una infraestructura ya destruida solo genera alertas inútiles.

### Verificado en AWS

| Prueba | Resultado |
|---|---|
| Pipeline completo | `SUCCEEDED` en **7 min 4 s** |
| Recorrido | Bronze → Silver → LeerInforme → Choice → Gold → Success |
| Con un lote corrupto (`--dirt-factor 20`) | `FAILED` con error `CalidadInsuficiente` |
| Recorrido en ese caso | Bronze → Silver → LeerInforme → Choice → **AvisarCalidad** → Fail |
| ¿Se construyó Gold? | **No.** Gold no aparece en el histórico de ejecución. |

La causa del corte fue `orders` al 7,69% frente a su umbral del 5%.

---

## CI/CD

Tres workflows en `.github/workflows/`:

| Workflow | Cuándo | Qué hace |
|---|---|---|
| `ci.yml` | cada PR y cada push a `develop`/`main` | lint, 97 tests unitarios, 65 de infraestructura y `cdk synth` |
| `deploy-dev.yml` | push a `develop` / manual | `cdk diff` siempre; despliega solo si se lo pides |
| `deploy-prod.yml` | push a `main` / manual | igual, más aprobación manual obligatoria |

### Sin claves de acceso: OIDC

Lo habitual es crear un usuario IAM, sacarle una clave y pegarla en los secrets
del repositorio. Esa clave no caduca, vive en un sistema de terceros y no hay
forma de saber quién la ha copiado.

Con OIDC no hay clave que guardar. GitHub firma un token de vida cortísima que
describe quién ejecuta qué, AWS lo valida contra el certificado público de
GitHub y devuelve credenciales temporales.

**La línea que lo sostiene todo es la condición sobre el claim `sub`.** El
emisor y el `aud` son idénticos para *todos* los repositorios de GitHub. Si el
trust policy solo comprobara esos dos, cualquiera podría crear un repositorio,
copiar el ARN del rol —que está escrito en el workflow, y **este repo es
público**—, pedir `id-token: write` y entrar en la cuenta. En CloudTrail se
vería un `AssumeRoleWithWebIdentity` perfectamente legítimo.

Y el error opuesto, igual de común: `repo:owner/repo:*`. Ese comodín incluye los
`pull_request`, y el token de un PR se emite contra el repositorio base, así que
un PR desde un fork desplegaría con tu rol. Los valores van **enumerados y
literales**, y hay un test que lo comprueba.

**El rol de prod confía en `environment:prod`, no en `refs/heads/main`.** Ese
claim solo aparece si el job declara `environment: prod`, que es lo único que
dispara la regla de revisor obligatorio. Aceptando también la rama, un job sin
`environment:` desplegaría producción saltándose la aprobación: la puerta
seguiría pintada en la interfaz sin cerrar nada.

**El rol no despliega nada por sí mismo**: solo sabe asumir los roles que creó
`cdk bootstrap`. Colgarle `AdministratorAccess` funcionaría igual de bien y
convertiría un push malicioso a `develop` en control total de la cuenta.

### El CI no puede quedarse verde mintiendo

`tests/conftest.py` y seis ficheros de test usan `pytest.importorskip("pyspark")`.
En tu portátil es una comodidad. En un runner sin el contenedor de Glue es una
mentira: un *skip* de colección no cambia el código de salida, así que pytest
termina en 0, el check sale verde y se han ejecutado 20 de los 97 tests.

Por eso el job corre dentro de `public.ecr.aws/glue/aws-glue-libs:5` —el mismo
runtime que AWS, y el único sitio donde están los JAR de Iceberg— y lleva
`PRACTICA_EXIGE_PYSPARK=1`, que convierte cualquier salto en fallo. Se comprueba
así:

```bash
python3 -m pytest tests/unit -m "not integration"      # verde, saltando 63 tests
PRACTICA_EXIGE_PYSPARK=1 python3 -m pytest tests/unit  # error, exit 4
make test-ci                                           # 97 passed en el contenedor
```

### Puesta en marcha (una sola vez)

El stack de OIDC es el huevo y la gallina: hay que desplegarlo a mano, porque
hasta que exista no hay rol que permita desplegar desde Actions. No pertenece a
ningún entorno, es gratis y **no debe borrarse** con `make destroy-dev`: por eso
solo se construye con `-c cicd=true` y así queda fuera del `--all`.

```bash
make diff-cicd          # revisa el trust policy A MANO antes de crear nada
make deploy-cicd        # OIDC_EXISTENTE=1 si tu cuenta ya tiene el proveedor

REPO=vicentesanchezrincon/practica
gh variable set AWS_REGION --repo $REPO --body eu-west-1
gh secret set AWS_ROLE_DEV_ARN --repo $REPO --body "arn:aws:iam::<cuenta>:role/practica-github-dev"
```

El ARN va como *secret* y no como *variable* porque lleva dentro el id de
cuenta y el repositorio es público.

### El interruptor de coste

Al mergear en `develop`, el workflow solo ejecuta `cdk diff`: lee AWS y cuenta
qué cambiaría, sin crear nada. El despliegue real hay que pedirlo. Para pasar a
despliegue continuo de verdad:

```bash
gh variable set DESPLIEGUE_AUTOMATICO_DEV --body true
```

Es una variable y no un cambio en el YAML a propósito: se activa o se revierte
en segundos, sin PR y sin protección de rama de por medio. Esta infraestructura
cuesta ~0,05 USD/hora, y un merge no debería poder resucitarla sin que lo pidas.

---

## Documentación

Dos documentos, con propósitos distintos, escritos en Markdown y generados a PDF:

| Fuente | Sale | Qué contiene |
|---|---|---|
| [docs/proyecto.md](docs/proyecto.md) | `documentacion-practica.pdf` | **Este** proyecto: arquitectura, decisiones, problemas vividos, las 9 fases |
| [docs/glosario.md](docs/glosario.md) | `glosario.pdf` | Conceptos reutilizables: siglas, arquitecturas, modelado, modos de fallo |

La frontera es *caso concreto vs patrón general*. «Se eligió Iceberg porque…»
va al documento del proyecto; «qué es un table format y en qué se diferencian
Iceberg, Delta y Hudi» va al glosario. Un incidente vivido aparece en los dos,
con enfoques distintos.

```bash
make docs          # genera los dos PDF
make docs DOC=glosario
```

La cadena es Markdown → HTML → PDF con Chrome headless. Markdown y no HTML a
mano por tres razones: los documentos se leen en GitHub sin generar nada, los
diff de un PR son legibles, y **el índice lo genera la herramienta** — el que
había escrito a mano llevaba tiempo desincronizado del cuerpo.

Los PDF y el HTML intermedio están en `.gitignore`: son megabytes de binario que
se regeneran en cada edición, y el hook `check-added-large-files` los rechazaría.

---

## Problemas conocidos

### `permission denied` en `/var/run/docker.sock`

Te añadiste al grupo `docker` pero tu sesión de shell sigue con los grupos
antiguos: `usermod -aG` no afecta a sesiones ya abiertas. Comprueba la diferencia:

```bash
id                    # ¿aparece "docker" aquí?
getent group docker   # ¿apareces tú aquí?
```

Si estás en el segundo pero no en el primero, cierra sesión y vuelve a entrar
(o reinicia). Para arreglarlo solo en la terminal actual, sin cerrar sesión:

```bash
newgrp docker
```

### `ruff`/`pytest` fallan al escribir un fichero concreto

Síntoma: `make format` deja un fichero sin reformatear y `make lint` sigue
quejándose del mismo, una y otra vez.

Causa: ese fichero se creó dentro de una shell abierta con `newgrp docker`, así
que heredó el grupo **`docker`** en vez del tuyo. El contenedor corre con tu GID
real, no con el de docker, y no puede escribirlo.

```bash
ls -l el/fichero.py     # ¿el grupo es "docker"?
```

Solución, y comprobación de que no quedan más:

```bash
find . -path ./.git -prune -o -path ./infra/.venv -prune -o ! -group $(id -gn $(id -un)) -print
find . -path ./.git -prune -o -path ./infra/.venv -prune -o ! -group $(id -gn $(id -un)) \
  -exec chgrp $(id -gn $(id -un)) {} +
```

Para evitarlo: crea y edita ficheros del proyecto desde una terminal normal, no
desde una abierta con `newgrp docker`. Mejor aún, cierra sesión y vuelve a
entrar una vez para que el grupo `docker` sea permanente y no necesites `newgrp`.

### `DELETE_FAILED` al destruir el stack de red

Síntoma: `make destroy-dev` deja `Practica-Dev-Network` en `DELETE_FAILED`, y el
siguiente `make deploy-dev` se niega a continuar.

```
The following resource(s) failed to delete: [GlueSecurityGroup..., VpcisolatedSubnet1...]
The subnet 'subnet-...' has dependencies and cannot be deleted
```

Causa: cuando un job de Glue corre dentro de la VPC, Glue crea ENIs en tu
subred. Al destruir el stack, esas ENIs pueden sobrevivir unos minutos y
bloquean el borrado de la subred y del security group.

Diagnóstico:

```bash
aws ec2 describe-network-interfaces \
  --filters "Name=vpc-id,Values=<vpc-id>" \
  --query "NetworkInterfaces[].[NetworkInterfaceId,Status,Description]" --output text
```

Si aparecen en estado `available` con `Attached to Glue using role: ...`, están
huérfanas: bórralas y reintenta.

```bash
aws ec2 delete-network-interface --network-interface-id eni-xxxxx
aws cloudformation delete-stack --stack-name Practica-Dev-Network
aws cloudformation wait stack-delete-complete --stack-name Practica-Dev-Network
```

Para evitarlo: espera unos minutos entre el último `start-job-run` y el
`make destroy-dev`.

### Bronze se queda vacío después de un `destroy` + `deploy`

Síntoma: redespliegas, lanzas `make bronze` y trae **0 filas** sin dar ningún
error. Bronze se queda vacío.

Causa: los watermarks viven en SSM Parameter Store y **los crea el job, no
CloudFormation**. `cdk destroy` no los toca. Al redesplegar, el bucket es nuevo
y está vacío, pero los watermarks siguen diciendo "ya ingesté hasta ayer", así
que el incremental no encuentra nada pendiente.

Es la trampa de tener estado fuera de la infraestructura como código.

Solución: bórralos antes de volver a empezar.

```bash
make reset-watermarks
```

O reprocesa desde una fecha concreta:

```bash
aws ssm put-parameter --overwrite --type String \
  --name /practica/dev/watermark/orders \
  --value 2026-01-01T00:00:00+00:00
```

### El contenedor de Glue no puede escribir en el proyecto

El usuario `hadoop` de la imagen es uid **10000**, distinto del tuyo. El
`docker-compose.yml` lo resuelve arrancando el contenedor con tu GID
(`user: "10000:${HOST_GID}"`), y el `Makefile` exporta `HOST_GID` por ti.
Si lanzas `docker compose` a mano sin `make`, exporta la variable antes:

```bash
export HOST_GID=$(id -g)
```

---

## Git Flow

| Rama | Rol |
|---|---|
| `main` | Producción. Protegida. Solo recibe merges de `release/*` y `hotfix/*`. |
| `develop` | Integración. Rama por defecto. Protegida. |
| `feature/*` | Una por fase del proyecto. PR contra `develop`. |
| `release/*` | Preparación de versión: bump + CHANGELOG. Merge a `main` **y de vuelta a `develop`**. |
| `hotfix/*` | Sale de `main`, vuelve a `main` **y a `develop`**. |

```bash
git checkout develop && git pull
git checkout -b feature/lo-que-sea
# ... trabajo ...
gh pr create --base develop
```

---

## Estado del proyecto

- [x] **Fase 0** — Git Flow, protección de ramas
- [x] **Fase 1** — Entorno local: Docker, generador de datos, tooling
- [x] **Fase 2** — `feature/cdk-foundation`: VPC sin NAT, bucket S3, Glue Data Catalog
- [x] **Fase 3** — `feature/rds-and-connection`: RDS + Glue Connection + siembra
- [x] **Fase 4** — `feature/bronze-ingest`: extracción incremental por watermark
- [x] **Fase 5** — `feature/silver-iceberg`: limpieza, dedup, MERGE, cuarentena
- [x] **Fase 6** — `feature/gold-marts`: modelo estrella y SCD2
- [x] **Fase 7** — `feature/step-functions`: orquestación y gate de calidad
- [x] **Fase 8** — `feature/ci-cd`: GitHub Actions con OIDC
- [x] **Fase 9** — `release/1.0.0` y ejercicio de hotfix

El histórico de versiones está en [CHANGELOG.md](CHANGELOG.md).

---

## Coste

| Recurso | Coste si lo dejas desplegado |
|---|---|
| VPC, subredes, security groups | gratis |
| Endpoint **Gateway** de S3 | gratis |
| Endpoints de **interfaz** (Glue, Logs, Secrets, STS) | ~0,01 USD/h **por AZ** cada uno → **~29 USD/mes** en 1 AZ |
| NAT Gateway | ~33 USD/mes — **no lo usamos** |
| RDS `db.t4g.micro`, 20 GB | gratis los primeros 12 meses (free tier); después ~13 USD/mes |
| S3 | céntimos a este volumen |
| Glue 5.0 | 0,44 USD/DPU-hora, mínimo 2 DPU, facturación por minuto → ~0,05–0,15 USD por ejecución completa |

Cuidado con la aritmética de los endpoints de interfaz: se paga **por endpoint y
por AZ**. Cuatro endpoints en 2 AZ son ~58 USD/mes, más caro que el NAT Gateway
que estamos evitando. Por eso `NetworkStack` los despliega en **una sola AZ**
(el tráfico entre AZ de PrivateLink es gratis desde 2022) y se pueden apagar:

```bash
cdk deploy Practica-Dev-Network -c environment=dev -c interface_endpoints=false
```

Lo anterior solo importa si dejas la infraestructura levantada. La forma correcta
de usar este laboratorio es destruirla al acabar, y entonces unas horas sueltas
cuestan céntimos:

```bash
make destroy-dev
```

Configura además un **AWS Budget con alerta a 5 USD** antes de desplegar nada.
