PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
DIST_DIR ?= $(CURDIR)/dist

.PHONY: check-python dependency-boundary-check license-check perf-public perf-public-concurrency \
	perf-public-preview-check perf-public-release-check \
	perf-sap-release-bridge-preview perf-sap-release-bridge-preview-check \
	perf-candidate-check perf-ecosystem \
	perf-ecosystem-rust4pm perf-ecosystem-ocpa perf-release-check \
	private-wheel private-wheel-verify

check-python:
	@$(PYTHON) -c 'import sys; sys.exit("Python 3.11 or newer is required (found %s)." % ".".join(map(str, sys.version_info[:3]))) if sys.version_info < (3, 11) else None'

dependency-boundary-check: check-python
	$(PYTHON) scripts/check-core-dependency-boundary.py

license-check: check-python
	$(PYTHON) scripts/check-licensing.py

private-wheel: check-python
	PYTHON=$(PYTHON) DIST_DIR=$(DIST_DIR) ./scripts/build-private-wheel.sh

private-wheel-verify: check-python
	@set -eu; \
	wheels="$$(find "$(DIST_DIR)" -maxdepth 1 -type f -name '*.whl' -print)"; \
	test -n "$$wheels" || { echo "no wheels in $(DIST_DIR)" >&2; exit 2; }; \
	for wheel in $$wheels; do $(PYTHON) scripts/verify-wheel.py "$$wheel"; done

perf-public: check-python
	PYTHON="$(PYTHON)" ./benchmarks/run_public_benchmark.sh

perf-public-concurrency: check-python
	PYTHON="$(PYTHON)" ./benchmarks/run_public_benchmark.sh --concurrency-only

perf-public-preview-check: check-python
	$(PYTHON) benchmarks/check_sap_release_regression.py \
		.benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json \
		--preview
	$(PYTHON) benchmarks/check_public_result.py \
		.benchmarks/public-common-pm-0.8.0.json \
		--release-bridge \
		.benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json \
		--preview
	$(PYTHON) benchmarks/check_sap_pm4py_result.py \
		.benchmarks/sap-pm4py-three-way-0.8.0.json \
		--release-bridge \
		.benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json \
		--preview
	$(PYTHON) benchmarks/check_public_provenance_pair.py \
		--common .benchmarks/public-common-pm-0.8.0.json \
		--sap .benchmarks/sap-pm4py-three-way-0.8.0.json \
		--preview

perf-sap-release-bridge-preview: check-python
	PYTHON="$(PYTHON)" ./benchmarks/run_sap_release_bridge.sh --preview

perf-sap-release-bridge-preview-check: check-python
	$(PYTHON) benchmarks/check_sap_release_regression.py \
		.benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json \
		--preview

perf-public-release-check: check-python
	$(PYTHON) benchmarks/check_public_result.py
	$(PYTHON) benchmarks/check_sap_pm4py_result.py
	$(PYTHON) benchmarks/check_public_provenance_pair.py

perf-ecosystem:
	./benchmarks/run_ecosystem_benchmark.sh

perf-ecosystem-rust4pm:
	./benchmarks/run_ecosystem_benchmark.sh --pair rust4pm

perf-ecosystem-ocpa:
	./benchmarks/run_ecosystem_benchmark.sh --pair ocpa

perf-release-check: perf-public-release-check

perf-candidate-run: check-python
	@test -n "$(BASELINE_SOURCE)" || \
		(echo "BASELINE_SOURCE is required" >&2; exit 2)
	./benchmarks/run_candidate_regression.sh \
		"$(BASELINE_SOURCE)" \
		"$(or $(CANDIDATE_RESULT),.benchmarks/candidate-regression.json)"

perf-candidate-check: check-python
	$(PYTHON) benchmarks/check_candidate_regression.py \
		"$(or $(CANDIDATE_RESULT),.benchmarks/candidate-regression.json)"
