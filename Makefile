COMPOSE ?= docker compose
SPARK_APP ?= tests/infra/apps/spark_ui_check.py
SPARK_APP_ARGS ?=
SPARK_CONF ?=
SPARK_UI_PORT ?= 4040
SPARK_SQL_FILE ?=
SPARK_SQL_VARS ?=
LANDING_ROOT ?= s3a://lake/landing
PROFILES ?=
PIPELINE ?=
CHECK ?= all
PIPELINES = landing bronze silver gold
GOLD_CHECKPOINTS ?= /buckets/lake/checkpoints/gold
MAINTENANCE_UI_PORT ?= 4044
GOLD_REBUILD_UI_PORT ?= 4045
.DEFAULT_GOAL := help

export BUILDX_NO_DEFAULT_ATTESTATIONS ?= 1

DRIVER_RUN = name="spark-app-run-$$(date +%s)-$$$$"; \
	trap 'docker stop "$$name" >/dev/null 2>&1 || true' EXIT INT TERM; \
	$(COMPOSE) run --rm --build --name "$$name" -p $(SPARK_UI_PORT):4040
DRIVER_CONF = --master "$$SPARK_MASTER_URL" \
	--conf spark.driver.host="$$(hostname -i)" \
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
CODE_MOUNTS = -v "$(CURDIR)/analytics:/opt/app/analytics" -v "$(CURDIR)/tests:/opt/app/tests"
RENDER_GOLD_DDL = uv run --frozen python -m pipelines.gold.ddl docker/analytics-db/initdb/02-gold-model.sql
APP_RUN = COMPOSE_PROJECT_NAME=$(CHECKS_PROJECT) $(COMPOSE) run --rm $(APP_BUILD) --no-deps $(CODE_MOUNTS)

PIPELINE_JOBS = $(addsuffix -job,$(if $(filter all,$(PIPELINE)),$(PIPELINES),$(filter $(PIPELINES),$(PIPELINE))))
PIPELINE_SERVICES = $(PIPELINE_JOBS) $(if $(filter silver all,$(PIPELINE)),lake-views)
CHECK_PIPELINE = $(if $(and $(PIPELINE_JOBS),$(filter 1,$(words $(PIPELINE)))),,$(error Set PIPELINE to one of: $(PIPELINES) all (got "$(PIPELINE)"), e.g. make $@ PIPELINE=landing))

.PHONY: help up down clean gold-reset db-logins gold-rebuild gold-retention spark-submit bronze-replay spark-sql spark-history data-quality lake-maintenance pipeline pipeline-logs pipeline-stop lake-views test lint format typecheck schemas test-infra hooks

help:
	@printf '%s\n' \
		'up             Render the gold DDL, build changed local images and start the default stack; add optional services with PROFILES (e.g. PROFILES="monitoring history").' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'gold-reset     Stop the gold job, recreate the gold schemas from the rendered DDL and delete the gold checkpoints (gold DATA LOSS; the lake is kept); requires CONFIRM=1.' \
		'db-logins      Create or update the service logins of the analytics database (members of gold_reader) from .env; safe to repeat.' \
		'gold-rebuild   Run gold-reset, then reload gold from the silver tables in one batch up to TO_TIME (ISO 8601 source commit time, inclusive; default: everything) and apply retention; the gold job stays stopped; requires CONFIRM=1.' \
		'gold-retention Drop the gold history months outside GOLD_RETENTION_MONTHS (default 36) and create the months ahead; the maintenance profile runs it every GOLD_RETENTION_INTERVAL_HOURS (default 24).' \
		'spark-submit   Run SPARK_APP (repo path; default: UI check job) in its own driver container; options SPARK_APP_ARGS, SPARK_CONF, SPARK_UI_PORT.' \
		'bronze-replay  Append the changes of a landing range that bronze lacks: TOPIC plus FROM_OFFSET and TO_OFFSET (optional PARTITION) or FROM_TIME and TO_TIME.' \
		'spark-sql      Open the Spark SQL shell in its own driver container; SPARK_SQL_FILE runs a file instead; SPARK_SQL_VARS (key=value ...) and LANDING_ROOT set its variables.' \
		'spark-history  Toggle the optional Spark History Server (localhost:18080): start it if stopped, stop it if running.' \
		'data-quality   Run the CHECK data quality checks against the running stack (landing, bronze, gold or all); fails if any check fails.' \
		'lake-maintenance  Compact the bronze and silver Iceberg tables, expire old snapshots and remove orphan files; fails if any table fails.' \
		'pipeline       Start the stack, the History Server and the PIPELINE job (landing, bronze, silver, gold or all) as a long-running service.' \
		'               silver and all also run lake-views once the silver tables exist.' \
		'pipeline-logs  Follow the logs of the PIPELINE job (landing, bronze, silver, gold or all).' \
		'pipeline-stop  Stop and remove the PIPELINE job (landing, bronze, silver, gold or all); the rest of the stack keeps running.' \
		'lake-views     Create the silver and gold views, or replace those whose query or descriptions changed; fails while a silver table is missing.' \
		'test           Run the unit tests with coverage (fails below 90%) in the app container.' \
		'lint           Check linting and formatting with ruff in the app container.' \
		'format         Format the code and apply safe lint fixes with ruff.' \
		'typecheck      Check types with ty in the app container; any diagnostic fails.' \
		'schemas        Render the full flat Avro schema of every lake table and view to target/schemas/<layer>/<table>.avsc, for reading.' \
		'               Build output: git-ignored, regenerated each run; edit analytics/pipelines/src/pipelines/schemas/ instead.' \
		'test-infra     Run the infra tests against an isolated copy of the stack (local only, a few minutes).' \
		'hooks          Install the pre-commit hooks in this clone (once).' \
		'Optional profiles: monitoring (Conduktor), history (Spark History Server), maintenance (lake maintenance and gold retention every 24 hours). See README.md.'

up:
	@$(RENDER_GOLD_DDL)
	@$(COMPOSE) $(addprefix --profile ,$(PROFILES)) up -d --build
	@$(COMPOSE) up -d --wait analytics-db
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
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/bronze/replay.py \
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

lake-maintenance:
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/maintenance/job.py \
		SPARK_UI_PORT=$(MAINTENANCE_UI_PORT) SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)"

pipeline:
	$(CHECK_PIPELINE)@$(RENDER_GOLD_DDL)
	@$(COMPOSE) --profile history up -d --build
	@$(COMPOSE) --profile app build spark-app
	@$(COMPOSE) up -d --wait analytics-db
	@$(MAKE) --no-print-directory gold-retention
	@$(COMPOSE) --profile pipeline up -d $(PIPELINE_SERVICES)

pipeline-logs:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile pipeline logs -f $(PIPELINE_SERVICES)

pipeline-stop:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile pipeline rm -sf $(PIPELINE_SERVICES)

lake-views:
	@$(COMPOSE) --profile app build spark-app
	@$(COMPOSE) --profile pipeline run --rm --no-deps lake-views

test:
	@$(APP_RUN) spark-app pytest -p no:cacheprovider

lint:
	@$(APP_RUN) spark-app sh -c 'ruff check && ruff format --check'

format:
	@$(APP_RUN) --user "$$(id -u):$$(id -g)" spark-app sh -c 'ruff format && ruff check --fix'

typecheck:
	@$(APP_RUN) spark-app ty check

schemas:
	@uv run --frozen python -m pipelines.schemas.render target/schemas

test-infra:
	@$(RENDER_GOLD_DDL)
	@uv run --frozen pytest tests/infra --no-cov -p no:cacheprovider -v

hooks:
	@uv run --frozen pre-commit install

gold-reset:
ifeq ($(CONFIRM),1)
	@$(RENDER_GOLD_DDL)
	@$(COMPOSE) --profile pipeline rm -sf gold-job
	@awk '{ print } /^BEGIN;$$/ && !dropped { print "DROP SCHEMA IF EXISTS gold, gold_internal CASCADE;"; dropped = 1 }' \
		docker/analytics-db/initdb/02-gold-model.sql \
		| $(COMPOSE) exec -T analytics-db sh -c 'psql -q -o /dev/null -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f -'
	@$(COMPOSE) exec -T seaweedfs curl -sf -X DELETE "http://localhost:8888$(GOLD_CHECKPOINTS)?recursive=true"
else
	@printf '%s\n' "Refusing gold reset: this deletes all gold data and the gold checkpoints. Run make gold-reset CONFIRM=1 to confirm." >&2
	@exit 1
endif

gold-rebuild:
ifeq ($(CONFIRM),1)
	@$(MAKE) --no-print-directory gold-reset CONFIRM=1
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/gold/rebuild.py \
		SPARK_UI_PORT=$(GOLD_REBUILD_UI_PORT) SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)" \
		SPARK_APP_ARGS='$(if $(TO_TIME),--to-time $(TO_TIME))'
	@printf '%s\n' "The gold job stays stopped; make pipeline PIPELINE=gold resumes streaming from silver."
else
	@printf '%s\n' "Refusing gold rebuild: this deletes all gold data and the gold checkpoints, then reloads gold from silver. Run make gold-rebuild CONFIRM=1 to confirm." >&2
	@exit 1
endif

gold-retention:
	@$(COMPOSE) run --rm --build --no-deps spark-app python -m pipelines.gold.retention

db-logins:
	@$(COMPOSE) exec -T analytics-db sh /docker-entrypoint-initdb.d/03-service-logins.sh

clean:
ifeq ($(CONFIRM),1)
	@$(COMPOSE) --profile '*' down --volumes
else
	@printf '%s\n' "Refusing cleanup: this deletes this project's containers and volumes, including persisted data. Run make clean CONFIRM=1 to confirm." >&2
	@exit 1
endif
