LAKE ?= data/lake
DATASETS ?= ds000001 ds000117 ds002718

.PHONY: install demo openneuro audit history transform report test lint docs airflow clean

install:            ## editable install with dev tools
	pip install -e ".[dev]"

demo:               ## offline run on the synthetic fixtures
	neurolake demo --lake $(LAKE)

openneuro:          ## ingest real OpenNeuro datasets (GitHub mirrors), then build and report
	neurolake ingest-mirror $(DATASETS) --lake $(LAKE)
	neurolake transform --lake $(LAKE)
	neurolake report --lake $(LAKE)

audit:              ## reproduce the OpenNeuro audit in docs/ (about 150 datasets)
	neurolake discover-mirrors --sample 150 --max-id 7500 --seed 2026 \
	    --log-file audit/probe_log.csv > audit/sample_ids.txt
	neurolake ingest-mirror --from-file audit/sample_ids.txt --workers 6 --lake data/audit
	neurolake transform --lake data/audit
	python audit/analyze.py data/audit
	python audit/render.py

history:            ## first vs latest snapshot of every audited dataset (run after `make audit`)
	python audit/history.py pairs
	python audit/history.py ingest
	python audit/history.py analyze
	python audit/render.py

transform:
	neurolake transform --lake $(LAKE)

report:
	neurolake report --lake $(LAKE)

test:
	pytest -q

lint:
	ruff check . && ruff format --check .

docs:               ## dbt docs site with lineage graph
	cd dbt && NEUROLAKE_LAKE=$(abspath $(LAKE)) dbt docs generate --profiles-dir . && \
	  NEUROLAKE_LAKE=$(abspath $(LAKE)) dbt docs serve --profiles-dir .

airflow:            ## local Airflow (needs `pip install apache-airflow` in this env)
	AIRFLOW_HOME=$(PWD)/.airflow AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False NEUROLAKE_LAKE=$(abspath $(LAKE)) \
	NEUROLAKE_REPORT=$(abspath data/report.html) airflow standalone

clean:
	rm -rf data dbt/target dbt/logs .airflow
