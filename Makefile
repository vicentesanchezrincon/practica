.DEFAULT_GOAL := help
SHELL := /bin/bash

# El contenedor necesita tu GID para poder escribir en el proyecto montado.
#
# "id -g" a secas NO sirve: devuelve el grupo primario, y dentro de una shell
# abierta con `newgrp docker` ese grupo pasa a ser "docker". El contenedor
# arrancaria con el gid de docker y no podria escribir en tus ficheros.
# "id -g $(id -un)" devuelve siempre el grupo real de /etc/passwd.
export HOST_GID := $(shell id -g $$(id -un))

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

# ruff vive en el contenedor (ver local/Dockerfile), no en el host: asi todo el
# mundo usa la misma version sin instalar nada.
.PHONY: lint
lint: ## Comprueba estilo y formato
	$(IN_GLUE) 'ruff check . && ruff format --check .'

.PHONY: format
format: ## Arregla estilo y formato
	$(IN_GLUE) 'ruff check --fix . && ruff format .'

# -------------------------------------------------------------- despliegue ---
# El CDK corre en el HOST (no en el contenedor): necesita el CLI de node y tus
# credenciales AWS. Los jobs de Spark corren en el contenedor. Son dos mundos.

ENV ?= dev
CDK := cd infra && . .venv/bin/activate && cdk

infra/.venv: infra/requirements.txt
	python3 -m venv infra/.venv
	infra/.venv/bin/pip install --quiet --upgrade pip
	infra/.venv/bin/pip install --quiet -r infra/requirements.txt
	@touch infra/.venv

.PHONY: infra-deps
infra-deps: infra/.venv ## Crea/actualiza el venv del CDK

.PHONY: test-infra
test-infra: infra/.venv ## Tests de la infraestructura (sobre la plantilla, sin tocar AWS)
	infra/.venv/bin/python -m pytest infra/tests -q

.PHONY: synth
synth: infra/.venv ## Genera el CloudFormation sin desplegar (ENV=dev|prod)
	$(CDK) synth -c environment=$(ENV)

.PHONY: diff
diff: infra/.venv ## Muestra que cambiaria en AWS
	$(CDK) diff --all -c environment=$(ENV)

.PHONY: deploy-dev
deploy-dev: infra/.venv ## Despliega todos los stacks en dev
	$(CDK) deploy --all -c environment=dev --require-approval never

.PHONY: destroy-dev
destroy-dev: infra/.venv ## Destruye los stacks de dev (hazlo al acabar cada sesion)
	$(CDK) destroy --all -c environment=dev --force
