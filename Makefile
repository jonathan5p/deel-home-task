COMPOSE ?= docker compose
SPARK_APP ?= tests/infra/apps/spark_ui_check.py
SPARK_APP_ARGS ?=
SPARK_CONF ?=
SPARK_UI_PORT ?= 4040
PROFILES ?=
.DEFAULT_GOAL := help

.PHONY: help up down clean spark-submit spark-history

help:
	@printf '%s\n' \
		'up             Build changed local images and start the default stack; add optional services with PROFILES (e.g. PROFILES="monitoring history").' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'spark-submit   Run SPARK_APP (repo path; default: UI check job) in its own driver container; options SPARK_APP_ARGS, SPARK_CONF, SPARK_UI_PORT.' \
		'spark-history  Start only the optional Spark History Server (localhost:18080).' \
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
		--py-files /opt/app/packages.zip \
		$(addprefix --conf ,$(SPARK_CONF)) \
		"$$@"' spark-submit $(SPARK_APP) $(SPARK_APP_ARGS)

spark-history:
	@$(COMPOSE) --profile history up -d --wait spark-history

clean:
ifeq ($(CONFIRM),1)
	@$(COMPOSE) --profile '*' down --volumes
else
	@printf '%s\n' "Refusing cleanup: this deletes this project's containers and volumes, including persisted data. Run make clean CONFIRM=1 to confirm." >&2
	@exit 1
endif
