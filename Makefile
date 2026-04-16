.PHONY: help setup generate load dbt-run dbt-test dbt-all dashboard pipeline docker-up docker-down clean

help: ## Afficher cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Installer les dépendances Python
	pip install -r requirements.txt

generate: ## Générer les données synthétiques d'assurance
	python ingestion/generate_synthetic_data.py

load: ## Charger les données brutes dans DuckDB
	python ingestion/loaders.py

dbt-run: ## Exécuter tous les modèles dbt
	cd dbt_project && dbt run

dbt-test: ## Lancer les tests dbt
	cd dbt_project && dbt test

dbt-all: dbt-run dbt-test ## Exécuter les modèles dbt puis les tests

dashboard: ## Lancer le dashboard Streamlit
	streamlit run dashboard/app.py

pipeline: generate load dbt-all ## Exécuter le pipeline complet en local

docker-up: ## Démarrer tous les services avec Docker Compose
	docker-compose up -d

docker-down: ## Arrêter tous les services Docker
	docker-compose down

clean: ## Supprimer les données générées et les artefacts dbt
	rm -rf data/raw/*.parquet data/warehouse.duckdb
	rm -rf dbt_project/target dbt_project/dbt_packages
