.PHONY: perf-public perf-public-check

perf-public:
	./benchmarks/run_public_benchmark.sh

perf-public-check:
	python3 benchmarks/check_public_result.py
	python3 benchmarks/check_sap_pm4py_result.py
