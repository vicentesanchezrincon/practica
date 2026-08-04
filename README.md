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
| `Practica-Dev-Glue` | Rol de ejecución de Glue, publicación de los scripts a S3 y el job `seed-rds` |

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
- [ ] **Fase 4** — `feature/bronze-ingest`: extracción incremental JDBC
- [ ] **Fase 5** — `feature/silver-iceberg`: limpieza, dedup, MERGE, cuarentena
- [ ] **Fase 6** — `feature/gold-marts`: modelo estrella y SCD2
- [ ] **Fase 7** — `feature/step-functions`: orquestación y gate de calidad
- [ ] **Fase 8** — `feature/ci-cd`: GitHub Actions con OIDC
- [ ] **Fase 9** — `release/1.0.0` y ejercicio de hotfix

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
