"""Exécuter les validations Great Expectations sur les données transformées.

Charge le contexte GE, exécute le checkpoint de qualité des données
et produit un rapport JSON dans data/metrics/.

Exemple :
    Lancer la validation après dbt run ::

        $ python scripts/run_great_expectations.py
"""

import json
import sys
from pathlib import Path

import great_expectations as gx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GE_ROOT = PROJECT_ROOT / "great_expectations"
METRICS_DIR = PROJECT_ROOT / "data" / "metrics"


def run_checkpoint(checkpoint_name: str = "claims_quality_checkpoint") -> dict:
    """Exécuter un checkpoint GE et retourner les résultats.

    Args:
        checkpoint_name: Nom du checkpoint défini dans great_expectations/.

    Returns:
        Dictionnaire avec les résultats de validation.
    """
    context = gx.get_context(context_root_dir=str(GE_ROOT))
    results = context.run_checkpoint(checkpoint_name=checkpoint_name)
    return results


def write_report(results: dict, output_path: Path) -> None:
    """Écrire un rapport JSON simplifié des résultats de validation.

    Args:
        results: Résultats bruts du checkpoint GE.
        output_path: Chemin du fichier de sortie.
    """
    report = {
        "success": results["success"],
        "statistics": {
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
        },
        "validation_results": [],
    }

    for validation_result in results.get("run_results", {}).values():
        vr = validation_result.get("validation_result", {})
        stats = vr.get("statistics", {})
        report["statistics"]["evaluated_expectations"] += stats.get(
            "evaluated_expectations", 0
        )
        report["statistics"]["successful_expectations"] += stats.get(
            "successful_expectations", 0
        )
        report["statistics"]["unsuccessful_expectations"] += stats.get(
            "unsuccessful_expectations", 0
        )
        report["validation_results"].append(
            {
                "success": vr.get("success", False),
                "expectation_suite_name": vr.get("meta", {}).get(
                    "expectation_suite_name", "unknown"
                ),
                "statistics": stats,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"Rapport écrit : {output_path}")


def main() -> int:
    """Point d'entrée principal.

    Returns:
        0 si toutes les validations passent, 1 sinon.
    """
    print("Exécution des validations Great Expectations...")

    try:
        results = run_checkpoint()
    except Exception as exc:
        print(f"Erreur lors de l'exécution du checkpoint : {exc}")
        # Écrire un rapport d'échec pour que DVC ait toujours sa métrique
        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        error_report = {"success": False, "error": str(exc)}
        (METRICS_DIR / "data_quality_report.json").write_text(
            json.dumps(error_report, indent=2)
        )
        return 1

    write_report(results, METRICS_DIR / "data_quality_report.json")

    if results["success"]:
        print("✓ Toutes les validations ont réussi.")
        return 0
    else:
        print("✗ Certaines validations ont échoué.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
