.PHONY: help setup generate load dbt-run dbt-test dbt-all dashboard pipeline docker-up docker-down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python dependencies
	pip install -r requirements.txt

generate: ## Generate synthetic insurance data
	python ingestion/generate_synthetic_data.py

load: ## Load raw data into DuckDB
	python ingestion/loaders.py

dbt-run: ## Run all dbt models
	cd dbt_project && dbt run

dbt-test: ## Run dbt tests
	cd dbt_project && dbt test

dbt-all: dbt-run dbt-test ## Run dbt models then tests

dashboard: ## Launch Streamlit dashboard
	streamlit run dashboard/app.py

pipeline: generate load dbt-all ## Run full pipeline locally

docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all Docker services
	docker-compose down

clean: ## Remove generated data and dbt artifacts
	rm -rf data/raw/*.parquet data/warehouse.duckdb
	rm -rf dbt_project/target dbt_project/dbt_packages
