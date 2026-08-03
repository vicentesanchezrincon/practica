.DEFAULT_GOAL := help
SHELL := /bin/bash

# El contenedor necesita tu GID para poder escribir en el proyecto montado.
export HOST_GID := $(shell id -g)

COMPOSE := docker compose --env-file .env -f local/docker-compose.yml
# Todo lo que sea PySpark se ejecuta DENTRO del contenedor de Glue,
# para usar exactamente el mismo runtime que AWS.
#
# "bash -lc" y no "bash -c": la imagen define SPARK_HOME, PATH y demas en el
# perfil de login. Sin -l, spark-submit y pyspark no se encuentran.
IN_GLUE := $(COMPOSE) exec -T glue bash -lc

.PHONY: help
help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- entorno ---

.env: .env.example
	@test -f .env || (cp .env.example .env && echo "Creado .env desde .env.example")

.PHONY: up
up: .env ## Levanta Postgres + contenedor Glue 5.0
	$(COMPOSE) up -d
	@echo "Listo. Entra con 'make shell' o siembra datos con 'make seed'."

.PHONY: down
down: ## Para los contenedores (conserva los datos)
	$(COMPOSE) down

.PHONY: clean
clean: ## Para los contenedores Y borra el volumen de Postgres
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Sigue los logs de los contenedores
	$(COMPOSE) logs -f

.PHONY: shell
shell: ## Abre una bash dentro del contenedor de Glue
	$(COMPOSE) exec glue bash -l

# Las credenciales se leen de las variables que ya tiene el propio contenedor
# de Postgres, asi no hay que mantenerlas sincronizadas en dos sitios.
PSQL := $(COMPOSE) exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

.PHONY: psql
psql: ## Abre una consola psql contra el Postgres local
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# ------------------------------------------------------------------ datos ---

.PHONY: schema
schema: ## Aplica el DDL en el Postgres local
	$(PSQL) < data_generator/schema.sql

.PHONY: seed
seed: schema ## Carga historica inicial de datos sinteticos
	$(IN_GLUE) 'python3 data_generator/seed.py --mode initial'

.PHONY: seed-daily
seed-daily: ## Simula un dia nuevo: inserts + updates + datos sucios
	$(IN_GLUE) 'python3 data_generator/seed.py --mode daily'

# ---------------------------------------------------------------- calidad ---

.PHONY: test
test: ## Tests unitarios (PySpark local, sin AWS)
	$(IN_GLUE) 'python3 -m pytest tests/unit -m "not integration"'

.PHONY: test-integration
test-integration: ## Tests de integracion (necesitan Postgres levantado)
	$(IN_GLUE) 'python3 -m pytest tests/integration -m integration'

.PHONY: lint
lint: ## Comprueba estilo y formato
	ruff check .
	ruff format --check .

.PHONY: format
format: ## Arregla estilo y formato
	ruff check --fix .
	ruff format .

# -------------------------------------------------------------- despliegue ---
# (se rellena en la Fase 2, cuando exista infra/)

.PHONY: synth
synth: ## cdk synth del entorno dev
	cd infra && cdk synth -c environment=dev

.PHONY: deploy-dev
deploy-dev: ## Despliega todos los stacks en dev
	cd infra && cdk deploy --all -c environment=dev --require-approval never

.PHONY: destroy-dev
destroy-dev: ## Destruye los stacks de dev (hazlo al acabar cada sesion)
	cd infra && cdk destroy --all -c environment=dev --force
