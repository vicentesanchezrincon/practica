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
- [ ] **Fase 2** — `feature/cdk-foundation`: VPC, S3, Glue Data Catalog
- [ ] **Fase 3** — `feature/rds-and-connection`: RDS + Glue Connection
- [ ] **Fase 4** — `feature/bronze-ingest`: extracción incremental JDBC
- [ ] **Fase 5** — `feature/silver-iceberg`: limpieza, dedup, MERGE, cuarentena
- [ ] **Fase 6** — `feature/gold-marts`: modelo estrella y SCD2
- [ ] **Fase 7** — `feature/step-functions`: orquestación y gate de calidad
- [ ] **Fase 8** — `feature/ci-cd`: GitHub Actions con OIDC
- [ ] **Fase 9** — `release/1.0.0` y ejercicio de hotfix

---

## Coste

El diseño evita el **NAT Gateway** (~32 USD/mes) usando VPC Endpoints, y RDS
`db.t4g.micro` entra en el free tier. Un run completo del pipeline cuesta unos
0,05–0,15 USD en Glue.

**Ejecuta `make destroy-dev` al terminar cada sesión de trabajo.**
