COMPOSE := ../compare/docker-compose.goodr-ocpm-clean.yml
CLIENT := docker compose -f $(COMPOSE) exec -T goodr_client
WARMUPS ?= 2
RUNS ?= 9
TIMEOUT_SECONDS ?= 120
RESULT_DIR := .benchmarks
VERTICALBAR_RESULT := $(RESULT_DIR)/verticalbar-goodr-three-way.json
COMMON_PM_RESULT := $(RESULT_DIR)/common-pm-goodr-three-way.json

.PHONY: perf-goodr perf-goodr-start perf-goodr-stop perf-goodr-verticalbar perf-goodr-common perf-goodr-check

perf-goodr: perf-goodr-check

perf-goodr-start:
	mkdir -p $(RESULT_DIR)
	docker compose -f $(COMPOSE) up -d

perf-goodr-stop:
	docker compose -f $(COMPOSE) stop

perf-goodr-verticalbar: perf-goodr-start
	$(CLIENT) python3 /dendrites/impl/ocpm-engine/benchmarks/goodr_verticalbar_three_way.py \
		--warmups $(WARMUPS) --runs $(RUNS) --timeout-seconds $(TIMEOUT_SECONDS) \
		--output /dendrites/impl/ocpm-engine/$(VERTICALBAR_RESULT)

perf-goodr-common: perf-goodr-start
	$(CLIENT) python3 /dendrites/impl/ocpm-engine/benchmarks/goodr_common_pm_three_way.py \
		--warmups $(WARMUPS) --runs $(RUNS) --timeout-seconds $(TIMEOUT_SECONDS) \
		--output /dendrites/impl/ocpm-engine/$(COMMON_PM_RESULT)

perf-goodr-check: perf-goodr-verticalbar perf-goodr-common
	python3 benchmarks/check_regression.py \
		--baseline docs/results/verticalbar-goodr-three-way-2026-07-18.json \
		--candidate $(VERTICALBAR_RESULT)
	python3 benchmarks/check_regression.py \
		--baseline docs/results/common-pm-goodr-three-way-2026-07-18.json \
		--candidate $(COMMON_PM_RESULT)
