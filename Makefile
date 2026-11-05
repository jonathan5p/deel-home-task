COMPOSE ?= docker compose
SPARK_MASTER_URL ?= spark://spark-master:7077
APP ?= /opt/spark/examples/src/main/python/pi.py
ARGS ?=
.DEFAULT_GOAL := help

.PHONY: help up down clean up-monitoring spark-submit

help:
	@printf '%s\n' \
		'up             Start the default stack in detached mode (monitoring stays optional).' \
		'up-monitoring  Start the default stack plus the optional Conduktor monitoring services.' \
		'down           Stop all project services, including optional profiles; keep volumes.' \
		'clean          Delete project containers and volumes (DATA LOSS); requires CONFIRM=1.' \
		'spark-submit   Submit APP (default: bundled Pi example) with ARGS to the Spark cluster.' \
		'See README.md for optional monitor controls.'

up:
	@$(COMPOSE) up -d

up-monitoring:
	@$(COMPOSE) --profile monitoring up -d

down:
	@$(COMPOSE) --profile '*' down

# The driver runs inside the master container; executors reach it back through that hostname.
spark-submit:
	@$(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master $(SPARK_MASTER_URL) \
		--conf spark.driver.host=spark-master \
		$(APP) $(ARGS)

clean:
ifeq ($(CONFIRM),1)
	@$(COMPOSE) --profile '*' down --volumes
else
	@printf '%s\n' "Refusing cleanup: this deletes this project's containers and volumes, including persisted data. Run make clean CONFIRM=1 to confirm." >&2
	@exit 1
endif
