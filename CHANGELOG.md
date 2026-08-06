# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y
versionado según [SemVer](https://semver.org/lang/es/).

Se escribe a mano y no se genera de los mensajes de commit. Un changelog útil
cuenta qué cambia **para quien usa el proyecto**, no qué ficheros se tocaron.
`fix(gold): divide por orders` es un mensaje de commit; «el ticket medio venía
mal desde la 1.0.0 y hay que reconstruir Gold» es una entrada de changelog.

## [No publicado]

## [1.0.0] — 2026-08-06

Primera versión completa: el pipeline va de Postgres a un modelo estrella
consultable en Athena, orquestado y con CI.

### Añadido

- **Entorno local reproducible** (Fase 1): Postgres 16 y el contenedor de Glue
  5.0 en Docker, generador de datos sintéticos con Faker en modos `initial` y
  `daily`, con suciedad deliberada para que la capa Silver tenga qué limpiar.
- **Red aislada y almacenamiento** (Fase 2): VPC sin salida a internet y **sin
  NAT Gateway** (VPC endpoints en su lugar, ~33 USD/mes de ahorro), bucket del
  data lake y las tres bases del Glue Data Catalog.
- **Origen en RDS** (Fase 3): Postgres en subredes aisladas con credenciales en
  Secrets Manager, Glue Connection para que los jobs alcancen la base de datos,
  y siembra por el rodeo Parquet → S3 → job de Glue, porque a una subred
  aislada no se llega con `psql`.
- **Capa Bronze** (Fase 4): extracción incremental por watermark en SSM
  Parameter Store, con columnas de linaje y escritura append-only. Reejecutar
  un día no duplica nada.
- **Capa Silver** (Fase 5): deduplicación por clave de negocio, normalización,
  validación declarativa por tabla y `MERGE INTO` sobre Iceberg. Las filas que
  fallan van a cuarentena, no se descartan en silencio.
- **Capa Gold** (Fase 6): modelo estrella con `dim_customer` en **SCD tipo 2**,
  claves subrogadas deterministas, miembro desconocido para no perder hechos
  huérfanos, y `agg_daily_sales`.
- **Orquestación** (Fase 7): máquina de estados de Step Functions con
  integración `.sync`, reintentos con backoff, alertas por SNS y una **puerta
  de calidad** que impide construir Gold sobre un lote malo.
- **CI/CD** (Fase 8): GitHub Actions con OIDC, sin ninguna clave de acceso
  guardada. Los tests unitarios corren dentro de la imagen de Glue y el CI
  falla si PySpark no está, en vez de saltárselos en silencio.

### Notas de operación

- La infraestructura se destruye con `make destroy-dev` al terminar. El stack
  `Practica-Cicd` queda fuera a propósito: es gratis y sin él no hay CI.
- El despliegue desde `develop` **no** es automático. Al mergear solo se
  ejecuta `cdk diff`; el despliegue real se pide a mano.

[No publicado]: https://github.com/vicentesanchezrincon/practica/compare/v1.0.0...develop
[1.0.0]: https://github.com/vicentesanchezrincon/practica/releases/tag/v1.0.0
