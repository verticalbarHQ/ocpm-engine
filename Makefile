PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: check-python perf-public perf-public-concurrency \
	perf-public-preview-check perf-public-release-check \
	perf-ocpq-preview-check perf-ocpq-release-check perf-release-check

check-python:
	@$(PYTHON) -c 'import sys; sys.exit("Python 3.11 or newer is required (found %s)." % ".".join(map(str, sys.version_info[:3]))) if sys.version_info < (3, 11) else None'

perf-public: check-python
	PYTHON="$(PYTHON)" ./benchmarks/run_public_benchmark.sh

perf-public-concurrency: check-python
	PYTHON="$(PYTHON)" ./benchmarks/run_public_benchmark.sh --concurrency-only

perf-public-preview-check: check-python
	$(PYTHON) benchmarks/check_public_result.py .benchmarks/public-common-pm-0.5.0.json --preview
	$(PYTHON) benchmarks/check_sap_pm4py_result.py .benchmarks/sap-pm4py-three-way-0.5.0.json --preview
	$(PYTHON) benchmarks/check_public_provenance_pair.py \
		--common .benchmarks/public-common-pm-0.5.0.json \
		--sap .benchmarks/sap-pm4py-three-way-0.5.0.json \
		--preview

perf-public-release-check: check-python
	$(PYTHON) benchmarks/check_public_result.py
	$(PYTHON) benchmarks/check_sap_pm4py_result.py
	$(PYTHON) benchmarks/check_public_provenance_pair.py

perf-ocpq-preview-check: check-python
	@reference=.benchmarks/ocpq-reproduced-0.6.7-preview.json; \
	candidate=.benchmarks/ocpq-bpic2017-0.5.0-host-provenance-preview.json; \
	memory=.benchmarks/ocpq-bpic2017-0.5.0-host-provenance-memory-preview.json; \
	for artifact in "$$reference" "$$candidate" "$$memory"; do \
		test -f "$$artifact" || { echo "missing preview artifact: $$artifact" >&2; exit 2; }; \
	done; \
	reference_sha="$$( $(PYTHON) -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$$reference" )"; \
	candidate_sha="$$( $(PYTHON) -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$$candidate" )"; \
	memory_sha="$$( $(PYTHON) -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$$memory" )"; \
	$(PYTHON) benchmarks/check_ocpq_result.py \
		--reference "$$reference" \
		--candidate "$$candidate" \
		--memory "$$memory" \
		--expected-reference-sha256 "$$reference_sha" \
		--expected-candidate-sha256 "$$candidate_sha" \
		--expected-memory-sha256 "$$memory_sha" \
		--allow-preview

perf-ocpq-release-check: check-python
	$(PYTHON) benchmarks/check_ocpq_result.py

perf-release-check: perf-public-release-check perf-ocpq-release-check
