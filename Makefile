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

# Los jobs de Glue importan src/common, pero Glue no ejecuta codigo local: hay
# que subirselo. --extra-py-files admite un .zip, asi que empaquetamos el
# paquete entero. Se reconstruye si cambia cualquier fichero de src/common.
build/common.zip: $(wildcard src/common/*.py)
	@mkdir -p build
	@find src/common -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	@rm -f build/common.zip
	cd src && python3 -m zipfile -c ../build/common.zip common
	@echo "Empaquetado build/common.zip"

.PHONY: build-common
build-common: build/common.zip ## Empaqueta src/common para los jobs de Glue

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
synth: infra/.venv build/common.zip ## Genera el CloudFormation sin desplegar (ENV=dev|prod)
	$(CDK) synth -c environment=$(ENV)

.PHONY: diff
diff: infra/.venv build/common.zip ## Muestra que cambiaria en AWS
	$(CDK) diff --all -c environment=$(ENV)

.PHONY: deploy-dev
deploy-dev: infra/.venv build/common.zip ## Despliega todos los stacks en dev
	$(CDK) deploy --all -c environment=dev --require-approval never

.PHONY: destroy-dev
destroy-dev: infra/.venv ## Destruye los stacks de dev (hazlo al acabar cada sesion)
	$(CDK) destroy --all -c environment=dev --force

# --------------------------------------------------------- siembra del RDS ---
# El RDS esta en subredes aisladas: no lo alcanzas con psql. El camino es
# Postgres local -> Parquet -> S3 -> job de Glue -> RDS.

# Los stacks se llaman Practica-Dev-* / Practica-Prod-*
ifeq ($(ENV),prod)
STACK_PREFIX := Practica-Prod
else
STACK_PREFIX := Practica-Dev
endif

# El nombre del bucket se lee del output del stack, para no tenerlo a mano.
BUCKET = $(shell aws cloudformation describe-stacks --stack-name $(STACK_PREFIX)-Storage \
	--query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text 2>/dev/null)

.PHONY: export-seed
export-seed: ## Exporta el Postgres local a Parquet y lo sube a S3
	@test -n "$(BUCKET)" || (echo "No encuentro el bucket. ¿Has hecho 'make deploy-dev'?" && exit 1)
	$(IN_GLUE) 'python3 data_generator/export_seed.py'
	aws s3 sync data/_seed "s3://$(BUCKET)/_seed" --delete
	@echo "Subido a s3://$(BUCKET)/_seed"

# Lanza un job de Glue, espera, y al terminar vuelca su salida.
#
# El `test -n` no es defensivo por gusto: si start-job-run falla (por ejemplo
# con ConcurrentRunsExceededException), devuelve vacio, y sin esta comprobacion
# el bucle se queda girando contra un run-id inexistente.
define run_glue_job
	@JOB=practica-$(ENV)-$(1); \
	RUN=$$(aws glue start-job-run --job-name $$JOB $(2) --query JobRunId --output text) || exit 1; \
	test -n "$$RUN" || { echo "No se pudo lanzar $$JOB"; exit 1; }; \
	echo "$$JOB lanzado (run $$RUN)"; \
	while true; do \
	  ESTADO=$$(aws glue get-job-run --job-name $$JOB --run-id $$RUN --query JobRun.JobRunState --output text); \
	  case $$ESTADO in \
	    SUCCEEDED) echo "OK: $$ESTADO";; \
	    FAILED|ERROR|TIMEOUT|STOPPED) echo "FALLO: $$ESTADO"; \
	      aws glue get-job-run --job-name $$JOB --run-id $$RUN --query JobRun.ErrorMessage --output text;; \
	    *) printf "  %s\r" $$ESTADO; sleep 15; continue;; \
	  esac; break; \
	done; \
	aws logs filter-log-events --log-group-name /aws-glue/jobs/output \
	  --log-stream-names "$$RUN" --query 'events[].message' --output text 2>/dev/null \
	  | tr '\t' '\n' | grep -E "^\s*\[" | sed 's/^\s*//' || true; \
	test "$$ESTADO" = SUCCEEDED
endef

.PHONY: seed-rds
seed-rds: ## Lanza el job de Glue que siembra el RDS y espera a que acabe
	$(call run_glue_job,seed-rds)

.PHONY: bronze
bronze: ## Ingesta incremental del RDS a la capa Bronze (TABLES=orders,... opcional)
	$(call run_glue_job,bronze-ingest,$(if $(TABLES),--arguments '{"--TABLES":"$(TABLES)"}'))

.PHONY: watermarks
watermarks: ## Muestra hasta donde llego la ultima ingesta de cada tabla
	@aws ssm get-parameters-by-path --path /practica/$(ENV)/watermark \
		--query 'Parameters[].[Name,Value]' --output table

.PHONY: db-creds
db-creds: ## Muestra las credenciales del RDS (estan en Secrets Manager)
	aws secretsmanager get-secret-value --secret-id practica/$(ENV)/postgres \
		--query SecretString --output text | python3 -m json.tool
