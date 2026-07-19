.PHONY: perf-public perf-public-check perf-ocpq-check

perf-public:
	./benchmarks/run_public_benchmark.sh

perf-public-check:
	python3 benchmarks/check_public_result.py
	python3 benchmarks/check_sap_pm4py_result.py
	python3 benchmarks/check_ocpq_result.py

perf-ocpq-check:
	python3 benchmarks/check_ocpq_result.py
