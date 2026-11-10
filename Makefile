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
PIPELINES = landing
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
APP_RUN = COMPOSE_PROJECT_NAME=$(CHECKS_PROJECT) $(COMPOSE) run --rm $(APP_BUILD) --no-deps $(CODE_MOUNTS)

PIPELINE_JOBS = $(addsuffix -job,$(if $(filter all,$(PIPELINE)),$(PIPELINES),$(filter $(PIPELINES),$(PIPELINE))))
CHECK_PIPELINE = $(if $(and $(PIPELINE_JOBS),$(filter 1,$(words $(PIPELINE)))),,$(error Set PIPELINE to one of: $(PIPELINES) all (got "$(PIPELINE)"), e.g. make $@ PIPELINE=landing))

.PHONY: help up down clean spark-submit spark-sql spark-history data-quality pipeline pipeline-logs pipeline-stop test lint format typecheck schemas test-infra hooks

help:
	@printf '%s\n' \
		'up             Build changed local images and start the default stack; add optional services with PROFILES (e.g. PROFILES="monitoring history").' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'spark-submit   Run SPARK_APP (repo path; default: UI check job) in its own driver container; options SPARK_APP_ARGS, SPARK_CONF, SPARK_UI_PORT.' \
		'spark-sql      Open the Spark SQL shell in its own driver container; SPARK_SQL_FILE runs a file instead; SPARK_SQL_VARS (key=value ...) and LANDING_ROOT set its variables.' \
		'spark-history  Toggle the optional Spark History Server (localhost:18080): start it if stopped, stop it if running.' \
		'data-quality   Run the data quality checks against the running stack (landing versus Kafka); fails if any check fails.' \
		'pipeline       Start the stack, the History Server and the PIPELINE job (landing, or all) as a long-running service.' \
		'pipeline-logs  Follow the logs of the PIPELINE job (landing, or all).' \
		'pipeline-stop  Stop and remove the PIPELINE job (landing, or all); the rest of the stack keeps running.' \
		'test           Run the unit tests with coverage (fails below 90%) in the app container.' \
		'lint           Check linting and formatting with ruff in the app container.' \
		'format         Format the code and apply safe lint fixes with ruff.' \
		'typecheck      Check types with ty in the app container; any diagnostic fails.' \
		'schemas        Render the full flat Avro schema of every lake table to target/schemas/<layer>/<table>.avsc, for reading.' \
		'               Build output: git-ignored, regenerated each run; edit analytics/pipelines/src/pipelines/schemas/ instead.' \
		'test-infra     Run the infra tests against an isolated copy of the stack (local only, a few minutes).' \
		'hooks          Install the pre-commit hooks in this clone (once).' \
		'Optional profiles: monitoring (Conduktor), history (Spark History Server). See README.md.'

up:
	@$(COMPOSE) $(addprefix --profile ,$(PROFILES)) up -d --build

down:
	@$(COMPOSE) --profile '*' down

spark-submit:
	@$(DRIVER_RUN) spark-app sh -c 'exec /opt/spark/bin/spark-submit $(DRIVER_CONF) \
		--py-files /opt/app/packages.zip \
		"$$@"' spark-submit $(SPARK_APP) $(SPARK_APP_ARGS)

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
	@$(MAKE) --no-print-directory spark-submit SPARK_APP=analytics/pipelines/src/pipelines/landing/quality_job.py \
		SPARK_UI_PORT=0 SPARK_CONF="spark.cores.max=2 $(SPARK_CONF)"

pipeline:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile history up -d --build
	@$(COMPOSE) --profile app build spark-app
	@$(COMPOSE) --profile pipeline up -d $(PIPELINE_JOBS)

pipeline-logs:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile pipeline logs -f $(PIPELINE_JOBS)

pipeline-stop:
	$(CHECK_PIPELINE)@$(COMPOSE) --profile pipeline rm -sf $(PIPELINE_JOBS)

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
	@uv run --frozen pytest tests/infra --no-cov -p no:cacheprovider -v

hooks:
	@uv run --frozen pre-commit install

clean:
ifeq ($(CONFIRM),1)
	@$(COMPOSE) --profile '*' down --volumes
else
	@printf '%s\n' "Refusing cleanup: this deletes this project's containers and volumes, including persisted data. Run make clean CONFIRM=1 to confirm." >&2
	@exit 1
endif
