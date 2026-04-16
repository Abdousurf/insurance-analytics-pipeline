"""Fixtures partagées pour les tests du pipeline."""

import pytest
import sys
from pathlib import Path

# Ajouter la racine du projet au PYTHONPATH pour les imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def sample_policies():
    """Générer un petit jeu de polices pour les tests (100 au lieu de 50k)."""
    from ingestion.generate_synthetic_data import generate_policies

    return generate_policies(n=100, start_date="2023-01-01")


@pytest.fixture
def sample_claims(sample_policies):
    """Générer des sinistres à partir des polices de test."""
    from ingestion.generate_synthetic_data import generate_claims

    return generate_claims(sample_policies)


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Créer une arborescence de données temporaire."""
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    return tmp_path
