"""DAG du pipeline d'analytique assurance.

Contrôle le flux de travail automatisé quotidien qui maintient nos données à jour.
Chaque matin à 6h00 (UTC), il exécute ces étapes dans l'ordre :
    1. generate_data : Créer des données fraîches de polices, sinistres et contrats.
    2. load_to_duckdb : Charger ces données brutes dans notre base de données locale.
    3. dbt_run_staging : Nettoyer et standardiser les données brutes.
    4. dbt_run_marts : Construire les tables de synthèse utilisées par le dashboard.
    5. dbt_test : Vérifier que les données sont correctes.
    6. notify_success : Journaliser la réussite de l'exécution.
"""

# ───────────────────────────────────────────────────────
# CE QUE FAIT CE FICHIER :
#
# Ce fichier définit un flux de travail automatisé quotidien (appelé « DAG »)
# avec Apache Airflow — un outil de planification et d'exécution de tâches.
#
# Considérez-le comme une recette avec des étapes à suivre dans l'ordre :
#   1. Créer des données de test fraîches
#   2. Les charger dans la base de données
#   3. Nettoyer et organiser les données (staging)
#   4. Construire les rapports de synthèse (marts)
#   5. Exécuter les contrôles de qualité
#   6. Journaliser la réussite de l'ensemble
#
# Si une étape échoue, elle est réessayée jusqu'à 3 fois et une alerte est envoyée.
# Ce flux s'exécute automatiquement chaque jour pour que le dashboard affiche
# toujours des informations à jour.
# ───────────────────────────────────────────────────────

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Chemins des dossiers sur le serveur Airflow où réside le code du projet
PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"

# Paramètres par défaut applicables à chaque tâche de ce flux
default_args = {
    "owner": "analytics-engineering",
    "depends_on_past": False,  # Chaque exécution est indépendante — ne pas attendre celle de la veille
    "email_on_failure": True,  # Envoyer un email en cas de problème
    "email_on_retry": False,  # Ne pas envoyer d'emails pour les réessais courants
    "retries": 3,  # Réessayer chaque étape jusqu'à 3 fois en cas d'échec
    "retry_delay": timedelta(minutes=5),  # Attendre 5 minutes avant de réessayer
    "retry_exponential_backoff": True,  # Augmenter le délai entre chaque tentative
}


def generate_data():
    """Créer des données d'assurance fraîches.

    Exécute le script de génération de données, qui produit des fichiers
    fictifs (mais réalistes) de polices, sinistres et contrats pour les tests.
    """
    # Ajouter le dossier du projet au chemin de recherche Python pour trouver notre code
    import sys

    sys.path.insert(0, PROJECT_DIR)
    from ingestion.generate_synthetic_data import main

    main()


def load_to_duckdb():
    """Charger les fichiers de données brutes dans notre base de données.

    Lit tous les fichiers de données fraîchement créés et les charge
    dans la base de données pour que les étapes suivantes puissent les exploiter.
    """
    # Ajouter le dossier du projet au chemin de recherche Python pour trouver notre code
    import sys

    sys.path.insert(0, PROJECT_DIR)
    from ingestion.loaders import load_all_raw_data

    results = load_all_raw_data()
    # Afficher un résumé de ce qui a été chargé
    for table, count in results.items():
        print(f"Chargé {table} : {count:,} lignes")


def notify_success(context):
    """Journaliser un message lorsque le pipeline se termine sans erreur.

    Args:
        context: Informations sur l'exécution en cours, fournies par Airflow.
    """
    print(f"Pipeline terminé avec succès à {datetime.now()}")


def notify_failure(context):
    """Journaliser un avertissement lorsqu'une étape du pipeline échoue.

    Args:
        context: Informations sur l'exécution en cours, fournies par Airflow.
    """
    task = context.get("task_instance")
    print(f"Pipeline ÉCHOUÉ : tâche={task.task_id}, date_exécution={context['ds']}")


# ── Définition du flux de travail (DAG) ─────────────────────────────────────
# Ce bloc configure le pipeline automatisé quotidien et toutes ses étapes
with DAG(
    dag_id="insurance_pipeline",
    default_args=default_args,
    description="Pipeline d'analytique assurance de bout en bout",
    schedule_interval="0 6 * * *",  # Exécution chaque jour à 6h00 UTC
    start_date=datetime(2024, 1, 1),  # Début de la planification à partir de cette date
    catchup=False,  # Ne pas tenter d'exécuter les dates passées manquées
    tags=["insurance", "analytics", "dbt"],
    on_failure_callback=notify_failure,
) as dag:

    # Étape 1 : Créer des données de test fraîches avec notre générateur Python
    task_generate = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    # Étape 2 : Charger les fichiers de données brutes dans la base
    task_load = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
    )

    # Étape 3 : Exécuter dbt pour nettoyer et standardiser les données brutes (couche staging)
    task_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging",
    )

    # Étape 4 : Exécuter dbt pour construire les tables de synthèse utilisées par le dashboard
    task_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate marts",
    )

    # Étape 5 : Exécuter les tests dbt pour vérifier que les données sont correctes
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    # Étape 6 : Journaliser la réussite de l'exécution
    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Définir l'ordre : chaque étape doit se terminer avant que la suivante ne commence
    (
        task_generate
        >> task_load
        >> task_dbt_staging
        >> task_dbt_marts
        >> task_dbt_test
        >> task_notify
    )
