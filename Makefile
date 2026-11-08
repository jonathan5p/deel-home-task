COMPOSE ?= docker compose
SPARK_APP ?= tests/infra/apps/spark_ui_check.py
SPARK_APP_ARGS ?=
SPARK_CONF ?=
SPARK_UI_PORT ?= 4040
PROFILES ?=
.DEFAULT_GOAL := help

# Code checks run in a throwaway app container with the working tree's source mounted,
# so they use the image's Spark and Python without a rebuild for every edit.
# APP_BUILD= reuses an already built app image, as CI does after building it with a layer cache.
APP_BUILD ?= --build
CODE_MOUNTS = -v "$(CURDIR)/analytics:/opt/app/analytics" -v "$(CURDIR)/tests:/opt/app/tests"
APP_RUN = $(COMPOSE) run --rm $(APP_BUILD) --no-deps $(CODE_MOUNTS)

.PHONY: help up down clean spark-submit spark-history test lint format typecheck test-infra hooks

help:
	@printf '%s\n' \
		'up             Build changed local images and start the default stack; add optional services with PROFILES (e.g. PROFILES="monitoring history").' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'spark-submit   Run SPARK_APP (repo path; default: UI check job) in its own driver container; options SPARK_APP_ARGS, SPARK_CONF, SPARK_UI_PORT.' \
		'spark-history  Toggle the optional Spark History Server (localhost:18080): start it if stopped, stop it if running.' \
		'test           Run the unit tests with coverage (fails below 90%) in the app container.' \
		'lint           Check linting and formatting with ruff in the app container.' \
		'format         Format the code and apply safe lint fixes with ruff.' \
		'typecheck      Check types with ty in the app container; any diagnostic fails.' \
		'test-infra     Run the infra tests against an isolated copy of the stack (local only, a few minutes).' \
		'hooks          Install the pre-commit hooks in this clone (once).' \
		'Optional profiles: monitoring (Conduktor), history (Spark History Server). See README.md.'

up:
	@$(COMPOSE) $(addprefix --profile ,$(PROFILES)) up -d --build

down:
	@$(COMPOSE) --profile '*' down

spark-submit:
	@name="spark-app-run-$$(date +%s)-$$$$"; \
	trap 'docker stop "$$name" >/dev/null 2>&1 || true' EXIT INT TERM; \
	$(COMPOSE) run --rm --build --name "$$name" -p $(SPARK_UI_PORT):4040 spark-app sh -c 'exec /opt/spark/bin/spark-submit \
		--master "$$SPARK_MASTER_URL" \
		--conf spark.driver.host="$$(hostname -i)" \
		--conf spark.hadoop.fs.s3a.access.key="$$LAKE_S3_ACCESS_KEY" \
		--conf spark.hadoop.fs.s3a.secret.key="$$LAKE_S3_SECRET_KEY" \
		--py-files /opt/app/packages.zip \
		$(addprefix --conf ,$(SPARK_CONF)) \
		"$$@"' spark-submit $(SPARK_APP) $(SPARK_APP_ARGS)

spark-history:
	@if [ -n "$$($(COMPOSE) --profile history ps -q --status running spark-history)" ]; then \
		$(COMPOSE) --profile history rm -sf spark-history >/dev/null 2>&1 \
			&& echo "Spark History Server stopped; event logs are kept."; \
	else \
		$(COMPOSE) --profile history up -d --wait spark-history \
			&& echo "Spark History Server running at http://localhost:18080"; \
	fi

test:
	@$(APP_RUN) spark-app pytest -p no:cacheprovider

lint:
	@$(APP_RUN) spark-app sh -c 'ruff check && ruff format --check'

format:
	@$(APP_RUN) --user "$$(id -u):$$(id -g)" spark-app sh -c 'ruff format && ruff check --fix'

typecheck:
	@$(APP_RUN) spark-app ty check

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
