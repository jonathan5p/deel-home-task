COMPOSE ?= docker compose
SPARK_APP ?= tests/infra/apps/spark_ui_check.py
SPARK_APP_ARGS ?=
SPARK_CONF ?=
SPARK_UI_PORT ?= 4040
SPARK_SQL_FILE ?=
SPARK_SQL_VARS ?=
LANDING_ROOT ?= s3a://lake/landing
DEFAULT_PROFILES ?= metrics
PROFILES ?=
UP_PROFILES = $(sort $(DEFAULT_PROFILES) $(PROFILES))
PIPELINE ?=
CHECK ?= all
PIPELINES = landing bronze silver gold
GOLD_CHECKPOINTS ?= /buckets/lake/checkpoints/gold
MAINTENANCE_UI_PORT ?= 4044
GOLD_REBUILD_UI_PORT ?= 4045
DRIVER_BUILD ?= --build
INFRA_WORKERS ?= 2
LAKE_FILER_URL ?= http://localhost:8888/buckets/lake
RESULTS ?=
LATENCY_SERVICES = landing-job bronze-job silver-job gold-job iceberg-rest
LATENCY_ENV = LAKE_CATALOG_URI LAKE_S3_ENDPOINT LAKE_S3_REGION SOURCE_DB_URL SOURCE_DB_USER SOURCE_DB_PASSWORD
.DEFAULT_GOAL := help

export BUILDX_NO_DEFAULT_ATTESTATIONS ?= 1

DRIVER_RUN = name="spark-app-run-$$(date +%s)-$$$$"; \
	trap 'docker stop "$$name" >/dev/null 2>&1 || true' EXIT INT TERM; \
	$(COMPOSE) run --rm --use-aliases $(DRIVER_BUILD) --name "$$name" -p $(SPARK_UI_PORT):4040
DRIVER_CONF = --master "$$SPARK_MASTER_URL" \
	--conf spark.driver.host="$$(hostname -i)" \
	--conf spark.executor.cores=1 \
	--conf spark.executor.memory=1g \
	--conf spark.hadoop.fs.s3a.access.key="$$LAKE_S3_ACCESS_KEY" \
	--conf spark.hadoop.fs.s3a.secret.key="$$LAKE_S3_SECRET_KEY" \
	$(addprefix --conf ,$(SPARK_CONF))

# Code checks run in a throwaway app container with the working tree's source mounted,
# so they use the image's Spark and Python without a rebuild for every edit.
# Each checkout gets its own project, and so its own image, so checks in parallel worktrees never share one.
# The project is set through COMPOSE_PROJECT_NAME rather than -p because the image names interpolate it.
# APP_BUILD= reuses an already built app image, as CI does after building it with a layer cache.
CHECKS_PROJECT ?= deel-checks-$(shell basename "$(CURDIR)" | LC_ALL=C tr 'A-Z' 'a-z' | LC_ALL=C sed 's/[^a-z0-9_-]/-/g')
APP_BUILD ?= --build
PYTEST_WORKERS ?= 4
CODE_MOUNTS = -v "$(CURDIR)/analytics:/opt/app/analytics" -v "$(CURDIR)/tests:/opt/app/tests" \
	-v "$(CURDIR)/monitoring:/opt/app/monitoring:ro" -v "$(CURDIR)/.env:/opt/app/.env:ro"
MKDOCS = uv run --frozen --group docs mkdocs
RENDER_GOLD_DDL = uv run --frozen python -m pipelines.lake.gold.ddl docker/analytics-db/initdb/02-gold-model.sql

SPARK_TESTS = analytics/common/tests analytics/pipelines/tests tests/grafana tests/prometheus
API_TESTS = analytics/api/tests analytics/common/tests
API_INFRA_APPS = tests/infra/apps/api_answers.py
# Each image's run keeps its coverage data on the host, so CI can combine them into one total.
# The folder is world-writable because the images run as their own users (spark, api).
COVERAGE_DIR = target/coverage
COVERAGE_MOUNT = -v "$(CURDIR)/$(COVERAGE_DIR):/opt/app/$(COVERAGE_DIR)"
APP_RUN = trap 'docker network rm $(CHECKS_PROJECT)_default >/dev/null 2>&1 || true' EXIT; trap 'exit 130' INT TERM; \
		  COMPOSE_PROJECT_NAME=$(CHECKS_PROJECT) $(COMPOSE) run --rm $(APP_BUILD) --no-deps $(CODE_MOUNTS)

PIPELINE_JOBS = $(addsuffix -job,$(if $(filter all,$(PIPELINE)),$(PIPELINES),$(filter $(PIPELINES),$(PIPELINE))))
PIPELINE_SERVICES = $(PIPELINE_JOBS) $(if $(filter silver all,$(PIPELINE)),lake-views)
CHECK_PIPELINE = $(if $(and $(PIPELINE_JOBS),$(filter 1,$(words $(PIPELINE)))),,$(error Set PIPELINE to one of: $(PIPELINES) all (got "$(PIPELINE)"), e.g. make $@ PIPELINE=landing))

.PHONY: help up down clean cdc-pause cdc-resume cdc-status gold-reset db-logins gold-rebuild gold-retention spark-submit bronze-replay spark-sql spark-history data-quality latency-probe lake-maintenance pipeline pipeline-logs pipeline-stop lake-views test lint format typecheck schemas docs docs-serve docs-results test-infra hooks

help:
	@printf '%s\n' \
		'up             Render the gold DDL, build changed local images and start the default stack with DEFAULT_PROFILES (default metrics; DEFAULT_PROFILES= leaves them out);' \
		'               add optional services with PROFILES (e.g. PROFILES="monitoring catalog").' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'gold-reset     Stop the gold job, recreate the gold schemas from the rendered DDL, drop the gold Iceberg tables and delete the gold checkpoints (gold DATA LOSS; silver is kept); requires CONFIRM=1.' \
		'db-logins      Create or update the service logins of the analytics database (members of gold_reader) from .env; safe to repeat.' \
		'gold-rebuild   Run gold-reset, then rebuild the gold Iceberg tables from silver in one batch up to TO_TIME (ISO 8601 source commit time, inclusive; default: everything), reload PostgreSQL from them and apply retention; the gold job stays stopped; requires CONFIRM=1.' \
		'gold-retention Drop the gold history months outside GOLD_RETENTION_MONTHS (default 36) and create the months ahead; the maintenance profile runs it every GOLD_RETENTION_INTERVAL_HOURS (default 24).' \
		'spark-submit   Run SPARK_APP (repo path; default: UI check job) in its own driver container; options SPARK_APP_ARGS, SPARK_CONF, SPARK_UI_PORT.' \
		'bronze-replay  Append the changes of a landing range that bronze lacks: TOPIC plus FROM_OFFSET and TO_OFFSET (optional PARTITION) or FROM_TIME and TO_TIME.' \
		'spark-sql      Open the Spark SQL shell in its own driver container; SPARK_SQL_FILE runs a file instead; SPARK_SQL_VARS (key=value ...) and LANDING_ROOT set its variables.' \
		'spark-history  Toggle the optional Spark History Server (localhost:18080): start it if stopped, stop it if running.' \
		'data-quality   Run the CHECK data quality checks against the running stack (landing, bronze, silver, gold or all); fails if any check fails; the data-quality profile runs them all every DATA_QUALITY_INTERVAL_HOURS (default 24).' \
		'latency-probe  Mark one source row per entity and time its change into landing, bronze, silver and PostgreSQL gold: ENTITIES (default all), PROBES (default 5),' \
		'               POLL_SECONDS (1), TIMEOUT_SECONDS per probe (300), MAX_LATENCY (e.g. gold=60,silver=40), KEYS (e.g. customers=7; default the lowest key);' \
		'               writes a JSON and an HTML report under <LAKE_ROOT>/latency/reports/; fails on a timeout or a threshold exceeded; needs the pipeline and the catalog profile.' \
		'lake-maintenance  Compact the bronze, silver and gold_internal Iceberg tables and their position delete files, expire old snapshots and remove orphan files; fails if any table fails.' \
		'pipeline       Start the stack, the History Server and the PIPELINE job (landing, bronze, silver, gold or all) as a long-running service.' \
		'               silver and all also run lake-views once the silver tables exist.' \
		'pipeline-logs  Follow the logs of the PIPELINE job (landing, bronze, silver, gold or all).' \
		'pipeline-stop  Stop and remove the PIPELINE job (landing, bronze, silver, gold or all); the rest of the stack keeps running.' \
		'cdc-pause      Pause the Debezium connector: no new source changes reach Kafka; the stack and the pipeline keep running.' \
		'cdc-resume     Resume the Debezium connector from where it paused, without losing changes.' \
		'cdc-status     Show the state of the Debezium connector and its task.' \
		'lake-views     Create the silver and gold views, or replace those whose query or descriptions changed; fails while a silver table is missing; skips the gold views until the gold job has created its tables.' \
		'test           Run the unit tests with coverage (fails below 90%), in PYTEST_WORKERS parallel workers (default 4): pipelines, common and the Grafana dashboard checks in the app image, the API and common in the API dev image.' \
		'lint           Check linting and formatting with ruff in the app container.' \
		'format         Format the code and apply safe lint fixes with ruff.' \
		'typecheck      Check types with ty in the app image and the API dev image; any diagnostic fails.' \
		'schemas        Render the full flat Avro schema of every lake table and view to target/schemas/<layer>/<table>.avsc, for reading.' \
		'               Build output: git-ignored, regenerated each run; edit analytics/pipelines/src/pipelines/schemas/ instead.' \
		'docs           Build the docs site (docs/ plus the table reference rendered from the schemas) into the git-ignored target/site/; warnings fail.' \
		'docs-serve     Serve the docs site on http://localhost:8001, rebuilding on every change.' \
		'docs-results   Copy the newest latency report (JSON and HTML) and data quality report from the lake into docs/results/, for the latest results pages;' \
		'               RESULTS picks latency or quality (default both); fails if seaweedfs is not running or a report is missing.' \
		'test-infra     Run the infra tests in INFRA_WORKERS parallel workers (default 2), each against its own isolated copy of the stack (local only, a few minutes; every worker needs the Docker memory of a full stack; 0 runs serially).' \
		'hooks          Install the pre-commit hooks in this clone (once).' \
		'Optional profiles: monitoring (Conduktor), history (Spark History Server), maintenance (lake maintenance and gold retention every 24 hours), data-quality (every data quality check every 24 hours), catalog (read-only Iceberg REST catalog on localhost:8181), metrics (started by make up unless DEFAULT_PROFILES is emptied: Prometheus on localhost:9090 scraping every Spark driver, the master, the workers and the exporter of completed stages and Iceberg table statistics, plus the History Server and the REST catalog). See README.md.'

up:
	@$(RENDER_GOLD_DDL)
ifneq ($(filter metrics,$(UP_PROFILES)),)
	@$(COMPOSE) --profile app build spark-app
endif
	@$(COMPOSE) $(addprefix --profile ,$(UP_PROFILES)) up -d --build
	@$(COMPOSE) up -d --wait analytics-db
ifneq ($(filter catalog metrics,$(UP_PROFILES)),)
	@$(MAKE) --no-print-directory db-logins
	@$(COMPOSE) --profile catalog restart iceberg-rest
	@$(COMPOSE) --profile catalog up -d --wait iceberg-rest
endif
	@$(MAKE) --no-print-directory gold-retention

down:
	@$(COMPOSE) --profile '*' down

spark-submit:
	@$(DRIVER_RUN) spark-app sh -c 'exec /opt/spark/bin/spark-submit $(DRIVER_CONF) \
		--py-files /opt/app/packages.zip \
		"$$@"' spark-submit $(SPARK_APP) $(SPARK_APP_ARGS)

REPLAY_UI_PORT ?= 4043
REPLAY_ARGS = --topic "$(TOPIC)" $(if $(PARTITION),--partition $(PARTITION)) \
	$(if $(FROM_OFFSET),--from-offset $(FROM_OFFSET)) $(if $(TO_OFFSET),--to-offset $(TO_OFFSET)) \
	$(if $(FROM_TIME),--from-time $(FROM_TIME)) $(if $(TO_TIME),--to-time $(TO_TIME))

bronze-replay:
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/lake/bronze/replay.py \
		SPARK_UI_PORT=$(REPLAY_UI_PORT) SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)" SPARK_APP_ARGS='$(strip $(REPLAY_ARGS))'

spark-sql:
	@$(DRIVER_RUN) -v "$(CURDIR)/tools:/opt/app/tools:ro" spark-app sh -c 'exec /opt/spark/bin/spark-sql $(DRIVER_CONF) \
		--conf spark.sql.catalogImplementation=in-memory \
		--hivevar landing_root="$(LANDING_ROOT)" \
		$(addprefix --hivevar ,$(SPARK_SQL_VARS)) \
		"$$@"' spark-sql $(if $(SPARK_SQL_FILE),-S -f $(SPARK_SQL_FILE))

spark-history:
	@if [ -n "$$($(COMPOSE) --profile history ps -q --status running spark-history)" ]; then \
		$(COMPOSE) --profile history rm -sf spark-history >/dev/null 2>&1 \
			&& echo "Spark History Server stopped; event logs are kept."; \
	else \
		$(COMPOSE) --profile history up -d --wait spark-history \
			&& echo "Spark History Server running at http://localhost:18080"; \
	fi

data-quality:
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/quality/job.py \
		SPARK_APP_ARGS="$(CHECK)" SPARK_UI_PORT=0 SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)"

LATENCY_ARGS = $(if $(ENTITIES),--entities $(ENTITIES)) $(if $(PROBES),--probes $(PROBES)) \
	$(if $(POLL_SECONDS),--poll-seconds $(POLL_SECONDS)) $(if $(TIMEOUT_SECONDS),--timeout-seconds $(TIMEOUT_SECONDS)) \
	$(if $(MAX_LATENCY),--max-latency $(MAX_LATENCY)) $(if $(KEYS),--keys $(KEYS))

latency-probe:
	@for service in $(LATENCY_SERVICES); do \
		if [ -z "$$($(COMPOSE) --profile pipeline --profile catalog ps -q --status running $$service)" ]; then \
			printf '%s\n' "$$service is not running; start it with: make up && make pipeline PIPELINE=all" >&2; \
			exit 2; \
		fi; \
	done
	@$(COMPOSE) run --rm --build --no-deps $(addprefix -e ,$(LATENCY_ENV)) spark-app \
		python -m pipelines.latency.job $(strip $(LATENCY_ARGS))

lake-maintenance:
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/maintenance/job.py \
		SPARK_UI_PORT=$(MAINTENANCE_UI_PORT) SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)"

pipeline:
	$(CHECK_PIPELINE)@$(RENDER_GOLD_DDL)
	@$(COMPOSE) --profile history up -d --build
	@$(COMPOSE) --profile app build spark-app
	@$(COMPOSE) up -d --wait analytics-db
ifneq ($(filter catalog,$(PROFILES)),)
	@$(MAKE) --no-print-directory db-logins
	@$(COMPOSE) --profile catalog restart iceberg-rest
	@$(COMPOSE) --profile catalog up -d --wait iceberg-rest
endif
	@$(MAKE) --no-print-directory gold-retention
	@$(COMPOSE) --profile pipeline up -d $(PIPELINE_SERVICES)

pipeline-logs:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile pipeline logs -f $(PIPELINE_SERVICES)

pipeline-stop:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile pipeline rm -sf $(PIPELINE_SERVICES)

CDC_CONNECTOR = finance-db-connector
CONNECT_API = $(COMPOSE) exec -T kafka-connect curl -fsS

cdc-pause:
	@$(CONNECT_API) -X PUT http://localhost:8083/connectors/$(CDC_CONNECTOR)/pause
	@$(MAKE) --no-print-directory cdc-status

cdc-resume:
	@$(CONNECT_API) -X PUT http://localhost:8083/connectors/$(CDC_CONNECTOR)/resume
	@$(MAKE) --no-print-directory cdc-status

cdc-status:
	@$(CONNECT_API) http://localhost:8083/connectors/$(CDC_CONNECTOR)/status; echo

lake-views:
	@$(COMPOSE) --profile app build spark-app
	@$(COMPOSE) --profile pipeline run --rm --no-deps lake-views

test:
	@mkdir -p $(COVERAGE_DIR) && chmod a+rwx $(COVERAGE_DIR) && rm -f $(COVERAGE_DIR)/.coverage*
	@$(APP_RUN) $(COVERAGE_MOUNT) -e COVERAGE_FILE=/opt/app/$(COVERAGE_DIR)/.coverage.app \
		spark-app pytest -p no:cacheprovider -n $(PYTEST_WORKERS) --dist loadfile $(SPARK_TESTS)
	@$(APP_RUN) $(COVERAGE_MOUNT) -e COVERAGE_FILE=/opt/app/$(COVERAGE_DIR)/.coverage.api \
		api-dev pytest -p no:cacheprovider -n $(PYTEST_WORKERS) --dist loadfile $(API_TESTS)

lint:
	@$(APP_RUN) spark-app sh -c 'ruff check && ruff format --check'

format:
	@$(APP_RUN) --user "$$(id -u):$$(id -g)" spark-app sh -c 'ruff format && ruff check --fix'

typecheck:
	@$(APP_RUN) spark-app ty check --extra-search-path /opt/spark/python \
		-c 'src.exclude=["$(API_INFRA_APPS)"]' analytics/common analytics/pipelines tests
	@$(APP_RUN) api-dev ty check analytics/api analytics/common $(API_INFRA_APPS)

schemas:
	@uv run --frozen python -m pipelines.schemas.render target/schemas

docs:
	@$(MKDOCS) build --strict

docs-serve:
	@$(MKDOCS) serve

docs-results:
	@if [ -z "$$($(COMPOSE) ps -q --status running seaweedfs)" ]; then \
		printf '%s\n' "seaweedfs is not running; start it with: make up (or docker compose up -d seaweedfs)" >&2; \
		exit 2; \
	fi
	@uv run --frozen python -m pipelines.reports.latest --filer-url $(LAKE_FILER_URL) --out docs/results $(RESULTS)

test-infra:
	@$(RENDER_GOLD_DDL)
	@uv run --frozen pytest tests/infra --no-cov -p no:cacheprovider -v \
		-n $(INFRA_WORKERS) --dist loadscope --no-loadscope-reorder

hooks:
	@uv run --frozen pre-commit install

gold-reset:
ifeq ($(CONFIRM),1)
	@$(RENDER_GOLD_DDL)
	@$(COMPOSE) --profile pipeline rm -sf gold-job
	@awk '{ print } /^BEGIN;$$/ && !dropped { print "DROP SCHEMA IF EXISTS gold, gold_internal CASCADE;"; dropped = 1 }' \
		docker/analytics-db/initdb/02-gold-model.sql \
		| $(COMPOSE) exec -T analytics-db sh -c 'psql -q -o /dev/null -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f -'
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/lake/gold/reset.py \
		SPARK_UI_PORT=$(GOLD_REBUILD_UI_PORT)
	@$(COMPOSE) exec -T seaweedfs curl -sf -X DELETE "http://localhost:8888$(GOLD_CHECKPOINTS)?recursive=true"
else
	@printf '%s\n' "Refusing gold reset: this deletes all gold data and the gold checkpoints. Run make gold-reset CONFIRM=1 to confirm." >&2
	@exit 1
endif

gold-rebuild:
ifeq ($(CONFIRM),1)
	@$(MAKE) --no-print-directory gold-reset CONFIRM=1
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/lake/gold/rebuild.py \
		SPARK_UI_PORT=$(GOLD_REBUILD_UI_PORT) SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)" \
		SPARK_APP_ARGS='$(if $(TO_TIME),--to-time $(TO_TIME))'
	@printf '%s\n' "The gold job stays stopped; make pipeline PIPELINE=gold resumes streaming from silver."
else
	@printf '%s\n' "Refusing gold rebuild: this deletes all gold data and the gold checkpoints, then reloads gold from silver. Run make gold-rebuild CONFIRM=1 to confirm." >&2
	@exit 1
endif

gold-retention:
	@$(COMPOSE) run --rm --build --no-deps spark-app python -m pipelines.lake.gold.retention

db-logins:
	@$(COMPOSE) exec -T analytics-db sh /docker-entrypoint-initdb.d/03-service-logins.sh

clean:
ifeq ($(CONFIRM),1)
	@$(COMPOSE) --profile '*' down --volumes
else
	@printf '%s\n' "Refusing cleanup: this deletes this project's containers and volumes, including persisted data. Run make clean CONFIRM=1 to confirm." >&2
	@exit 1
endif
