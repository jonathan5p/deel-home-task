COMPOSE ?= docker compose
SPARK_APP ?= /opt/spark/jobs/spark_ui_check.py
SPARK_APP_ARGS ?=
PROFILES ?= $(COMPOSE_PROFILES)
.DEFAULT_GOAL := help

comma := ,
space := $(subst ,, )

.PHONY: help up down clean spark-submit spark-history

help:
	@printf '%s\n' \
		'up             Start the default stack; add optional services with PROFILES (e.g. PROFILES="monitoring history").' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'spark-submit   Submit SPARK_APP (default: UI check job, see README) with SPARK_APP_ARGS to the Spark cluster.' \
		'spark-history  Start only the optional Spark History Server (localhost:18080).' \
		'Optional profiles: monitoring (Conduktor), history (Spark History Server). See README.md.'

# Compose expects COMPOSE_PROFILES as a comma-separated list; PROFILES also accepts spaces.
up:
	@COMPOSE_PROFILES="$(subst $(space),$(comma),$(strip $(PROFILES)))" $(COMPOSE) up -d

down:
	@$(COMPOSE) --profile '*' down

# The driver runs inside the master container, so the master URL and the hostname executors
# connect back to are resolved from that container's environment rather than duplicated here.
spark-submit:
	@$(COMPOSE) exec -T spark-master sh -c '/opt/spark/bin/spark-submit \
		--master "$$SPARK_MASTER_URL" \
		--conf spark.driver.host="$$HOSTNAME" \
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
