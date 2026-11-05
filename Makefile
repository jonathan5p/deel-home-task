COMPOSE ?= docker compose
SPARK_APP ?= /opt/spark/examples/src/main/python/pi.py
SPARK_APP_ARGS ?=
.DEFAULT_GOAL := help

.PHONY: help up down clean up-monitoring spark-submit

help:
	@printf '%s\n' \
		'up             Start the default stack in detached mode (monitoring stays optional).' \
		'up-monitoring  Start the default stack plus the optional Conduktor monitoring services.' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'spark-submit   Submit SPARK_APP (default: bundled Pi example) with SPARK_APP_ARGS to the Spark cluster.' \
		'See README.md for optional monitor controls.'

up:
	@$(COMPOSE) up -d

up-monitoring:
	@$(COMPOSE) --profile monitoring up -d

down:
	@$(COMPOSE) --profile '*' down

# The driver runs inside the master container, so the master URL and the hostname executors
# connect back to are resolved from that container's environment rather than duplicated here.
spark-submit:
	@$(COMPOSE) exec -T spark-master sh -c '/opt/spark/bin/spark-submit \
		--master "$$SPARK_MASTER_URL" \
		--conf spark.driver.host="$$HOSTNAME" \
		"$$@"' spark-submit $(SPARK_APP) $(SPARK_APP_ARGS)

clean:
ifeq ($(CONFIRM),1)
	@$(COMPOSE) --profile '*' down --volumes
else
	@printf '%s\n' "Refusing cleanup: this deletes this project's containers and volumes, including persisted data. Run make clean CONFIRM=1 to confirm." >&2
	@exit 1
endif
