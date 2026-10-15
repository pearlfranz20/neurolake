LAKE ?= data/lake
DATASETS ?= ds000001 ds000117 ds002718

.PHONY: install demo openneuro transform report test lint docs airflow clean

install:            ## editable install with dev tools
	pip install -e ".[dev]"

demo:               ## offline run on the synthetic fixtures
	neurolake demo --lake $(LAKE)

openneuro:          ## ingest real OpenNeuro datasets, then build and report
	neurolake ingest-openneuro $(DATASETS) --lake $(LAKE)
	neurolake transform --lake $(LAKE)
	neurolake report --lake $(LAKE)

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
