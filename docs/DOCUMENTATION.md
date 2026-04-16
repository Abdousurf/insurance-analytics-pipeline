# Documentation technique & fonctionnelle
## Pipeline d'Analytique Assurance

---

## Table des matières

1. [Vue d'ensemble fonctionnelle](#1-vue-densemble-fonctionnelle)
2. [Architecture technique](#2-architecture-technique)
3. [Étape 1 — Génération des données synthétiques](#3-étape-1--génération-des-données-synthétiques)
4. [Étape 2 — Ingestion et stockage (DuckDB)](#4-étape-2--ingestion-et-stockage-duckdb)
5. [Étape 3 — Transformation avec dbt (Staging)](#5-étape-3--transformation-avec-dbt-staging)
6. [Étape 4 — Transformation avec dbt (Marts)](#6-étape-4--transformation-avec-dbt-marts)
7. [Étape 5 — Dashboard Streamlit](#7-étape-5--dashboard-streamlit)
8. [Étape 6 — Orchestration Airflow](#8-étape-6--orchestration-airflow)
9. [Glossaire actuariel](#9-glossaire-actuariel)

---

## 1. Vue d'ensemble fonctionnelle

### Contexte métier

Une compagnie d'assurance Non-Vie (IARD) doit surveiller en continu la rentabilité de son portefeuille. L'indicateur clé est le **ratio Sinistres/Primes (S/P)** — aussi appelé Loss Ratio — qui mesure la part des primes encaissées utilisée pour payer les sinistres.

```
S/P = Charge sinistres / Primes acquises

Interprétation :
  S/P < 70%  →  Portefeuille très profitable
  S/P 70–85% →  Zone normale (target marché)
  S/P > 85%  →  Alerte : portefeuille sous-tarifé ou sinistralité anormale
```

### Ce que fait ce pipeline

Ce projet automatise l'ensemble du cycle analytique :

```
Données brutes (polices, sinistres, traités réassurance)
        ↓
Nettoyage + standardisation (dbt staging)
        ↓
Calcul des indicateurs actuariels (dbt marts)
        ↓
Dashboard interactif avec alertes automatiques (Streamlit)
```

### Utilisateurs cibles

| Profil | Ce qu'il utilise |
|--------|-----------------|
| Actuaire / Souscripteur | Dashboard S/P par branche et région |
| Data Engineer | Pipeline dbt + orchestration Airflow |
| DSI / Management | KPIs agrégés, alertes segments hors cible |

---

## 2. Architecture technique

```
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION (Airflow)                   │
│           DAG : insurance_pipeline  [quotidien 06:00]        │
└──────────────────────────┬──────────────────────────────────┘
                           │
          ┌────────────────▼───────────────┐
          │   INGESTION (Python/Pandas)     │
          │  generate_synthetic_data.py     │
          │  → policies.parquet             │
          │  → claims.parquet              │
          │  → contracts.parquet           │
          └────────────────┬───────────────┘
                           │
          ┌────────────────▼───────────────┐
          │     WAREHOUSE (DuckDB)          │
          │  warehouse.duckdb               │
          │  Schéma : raw.*                 │
          └────────────────┬───────────────┘
                           │
          ┌────────────────▼───────────────┐
          │   STAGING (dbt)                 │
          │  stg_claims     ← nettoyage     │
          │  stg_policies   ← typage        │
          │  stg_contracts  ← normalisation │
          └────────────────┬───────────────┘
                           │
          ┌────────────────▼───────────────┐
          │   MARTS (dbt)                   │
          │  mart_loss_ratio                │
          │  mart_claims_frequency          │
          │  mart_portfolio_performance     │
          └────────────────┬───────────────┘
                           │
          ┌────────────────▼───────────────┐
          │   DASHBOARD (Streamlit)         │
          │  KPIs · Graphiques · Alertes    │
          └────────────────────────────────┘
```

**Stack complète :**

| Couche | Outil | Version |
|--------|-------|---------|
| Orchestration | Apache Airflow | 2.8 |
| Transformation | dbt-core + dbt-duckdb | 1.7 |
| Stockage | DuckDB | 0.10 |
| Ingestion | Python + Pandas + NumPy | 3.11 |
| Dashboard | Streamlit + Plotly | latest |
| Conteneurisation | Docker Compose | latest |

---

## 3. Étape 1 — Génération des données synthétiques

**Fichier :** `ingestion/generate_synthetic_data.py`

### Objectif fonctionnel

Produire un jeu de données P&C (Non-Vie) réaliste, sans données réelles clients, utilisable pour développer et démontrer le pipeline.

### Comment les données sont générées

#### 3.1 Paramètres actuariels par branche (LOB)

```python
LOB_PARAMS = {
    "Auto":      { "frequency": 0.07,  "avg_severity": 3_200  },
    "Home":      { "frequency": 0.04,  "avg_severity": 5_800  },
    "Liability": { "frequency": 0.025, "avg_severity": 12_000 },
    "Health":    { "frequency": 0.18,  "avg_severity": 1_400  },
}
```

- **frequency** = taux de sinistralité (sinistres déclarés / polices en portefeuille)
- **avg_severity** = coût moyen d'un sinistre en euros

Ces paramètres sont calibrés sur des références marché IARD françaises (S/P cible : 68–75%).

#### 3.2 Génération des polices (`generate_policies`)

Entrées :
- `n = 50 000` polices
- Dates d'effet réparties aléatoirement sur 2021–2024

Pour chaque police, le pipeline génère :

```
policy_id     → identifiant unique (POL0000001 ... POL0050000)
lob           → branche (Auto 45%, Home 30%, Liability 15%, Health 10%)
region        → région française (8 régions avec facteur de risque)
annual_premium → prime annuelle = base_range × risk_factor_region
channel       → canal de distribution (Direct, Broker, Online, Agent)
status        → Active / Expired selon la date d'expiration
```

**Facteur de risque régional :** Île-de-France × 1.25 (zone urbaine, accidentalité élevée), Bretagne × 0.88 (zone rurale, risque bas).

#### 3.3 Génération des sinistres (`generate_claims`)

Pour chaque police, le nombre de sinistres est tiré d'une **loi de Poisson** :

```
N_sinistres ~ Poisson(λ × facteur_régional)
```

Le coût de chaque sinistre est tiré d'une **loi log-normale** (standard actuariel pour modéliser la sévérité) :

```
Coût ~ LogNormal(μ = ln(avg_severity), σ = severity_std / avg_severity)
```

**Simulation de l'IBNR** (Incurred But Not Reported) :
- Le délai de déclaration est tiré d'une **loi exponentielle** de moyenne 15 jours
- Si ce délai dépasse 30 jours → flag `is_ibnr_candidate = True`
- L'IBNR est une réserve pour sinistres survenus mais pas encore déclarés — enjeu réglementaire majeur

#### 3.4 Génération des traités de réassurance

Trois traités sont simulés :

| Traité | Branche | Type | Rétention | Limite |
|--------|---------|------|-----------|--------|
| XL-AUTO-2023 | Auto | Excess of Loss | €100K | €500K |
| XL-HOME-2023 | Home | Excess of Loss | €250K | €2M |
| QUOTA-LIAB-2023 | Liability | Quota Share | 70% | — |

**Excess of Loss** : le réassureur prend en charge la part du sinistre au-delà de la rétention, jusqu'à la limite.
**Quota Share** : le réassureur prend 30% de chaque prime et chaque sinistre (cession proportionnelle).

#### 3.5 Sortie

```bash
python ingestion/generate_synthetic_data.py
# Produit :
#   data/raw/policies.parquet   (~50 000 lignes)
#   data/raw/claims.parquet     (~18 000 lignes estimées)
#   data/raw/contracts.parquet  (3 lignes)
```

Exemple de sortie console :
```
Auto         | S/P: 71.3% | Freq: 7.12% | Policies: 22,500
Home         | S/P: 68.9% | Freq: 4.02% | Policies: 15,000
Liability    | S/P: 73.1% | Freq: 2.54% | Policies: 7,500
Health       | S/P: 69.5% | Freq: 18.2% | Policies: 5,000
```

---

## 4. Étape 2 — Ingestion et stockage (DuckDB)

### Pourquoi DuckDB ?

DuckDB est une base analytique **in-process** (sans serveur), optimisée pour les requêtes OLAP. Elle lit nativement les fichiers Parquet et s'intègre parfaitement avec dbt.

```python
import duckdb
con = duckdb.connect("data/warehouse.duckdb")
con.execute("CREATE TABLE raw.claims AS SELECT * FROM 'data/raw/claims.parquet'")
```

**Avantages pour ce projet :**
- Pas de serveur à configurer
- Compatible BigQuery/Snowflake via remplacement du profil dbt
- Lecture Parquet optimisée en colonnes (50× plus rapide que Pandas pour les agrégations)

---

## 5. Étape 3 — Transformation avec dbt (Staging)

**Fichiers :** `dbt_project/models/staging/`

### Principe des modèles staging

Les modèles staging appliquent uniquement :
1. Le **renommage** des colonnes (snake_case standardisé)
2. Le **typage** explicite (DECIMAL pour les montants, DATE pour les dates)
3. La **normalisation** (lowercase, initcap)
4. Les **contrôles de qualité de base** (nulls, valeurs négatives)

Ils ne contiennent **aucune logique métier** — c'est la couche de données "propres".

### `stg_claims.sql` — Détail

```sql
-- Étape 1 : sélectionner la source brute
with source as (
    select * from {{ source('raw', 'claims') }}
),

-- Étape 2 : renommer + typer + enrichir
renamed as (
    select
        claim_id,
        policy_id,
        lower(lob) as line_of_business,           -- normalisation casse
        cast(ultimate_cost as decimal(15,2))       -- typage explicite
        cast(claim_date as date) as claim_date,

        -- Calcul dérivé : délai de déclaration
        datediff('day', claim_date, reporting_date) as reporting_lag_days,

        -- Flag IBNR : déclaration > 30 jours après le sinistre
        case when datediff(...) > 30 then true else false end as is_ibnr_candidate
    from source
    where claim_id is not null  -- contrôle intégrité
),

-- Étape 3 : validation des montants
validated as (
    select * from renamed
    where ultimate_cost_eur >= 0
      and paid_amount_eur <= ultimate_cost_eur * 1.05  -- tolérance 5% surpaiement
)

select * from validated
```

**Le flag IBNR** est généré ici car c'est une propriété intrinsèque du sinistre (pas une agrégation). Il sera utilisé dans le mart pour calculer le taux d'IBNR par segment.

### Tests dbt automatiques

```yaml
# dbt_project/models/staging/schema.yml
models:
  - name: stg_claims
    columns:
      - name: claim_id
        tests: [not_null, unique]
      - name: ultimate_cost_eur
        tests: [not_null, { dbt_utils.expression_is_true: "ultimate_cost_eur >= 0" }]
      - name: line_of_business
        tests: [accepted_values: {values: [auto, home, liability, health]}]
```

---

## 6. Étape 4 — Transformation avec dbt (Marts)

**Fichiers :** `dbt_project/models/marts/`

### `mart_loss_ratio.sql` — Le KPI central

Ce mart calcule le **S/P ratio** (Loss Ratio) par branche, région et année, en joignant les primes et les sinistres.

#### Logique en 4 blocs

**Bloc 1 — `policies_earned` :** agrégation des primes par segment
```sql
select line_of_business, region, inception_year as accident_year,
       count(policy_id) as policy_count,
       sum(annual_premium_eur) as earned_premium_eur
from stg_policies
group by 1, 2, 3
```

**Bloc 2 — `claims_incurred` :** agrégation des sinistres par segment
```sql
select line_of_business, region,
       date_part('year', claim_date) as accident_year,
       count(claim_id) as claim_count,
       sum(ultimate_cost_eur) as incurred_losses_eur,
       count(case when is_ibnr_candidate then 1 end) as ibnr_claims_count
from stg_claims
group by 1, 2, 3
```

**Bloc 3 — `combined` :** jointure FULL OUTER (pour conserver les segments sans sinistres ou sans primes)

**Bloc 4 — `final` :** calcul des ratios

```sql
-- Loss Ratio (S/P)
round(incurred_losses_eur / earned_premium_eur, 4)  as loss_ratio,

-- Fréquence sinistres
round(claim_count::float / policy_count, 4)          as claims_frequency,

-- Sévérité moyenne
round(incurred_losses_eur / claim_count, 2)          as avg_severity_eur,

-- Taux IBNR
round(ibnr_claims_count::float / claim_count, 4)     as ibnr_rate,

-- Alerte : S/P > 85%
case when incurred/premium > 0.85 then true else false end as high_loss_ratio_flag
```

#### Matérialisation

```sql
{{ config(materialized='table', tags=['actuary', 'kpi', 'daily']) }}
```

Le mart est matérialisé en **table** (pas en view) car il est interrogé fréquemment par le dashboard. Il est tagué `daily` pour être rafraîchi quotidiennement par Airflow.

---

## 7. Étape 5 — Dashboard Streamlit

**Fichier :** `dashboard/app.py`

### Fonctionnalités

#### Filtres (sidebar)
- **Branche (LOB)** : All / Auto / Home / Liability / Health
- **Années d'accident** : multi-sélection
- **Mode alerte** : afficher uniquement les segments S/P > 85%

#### KPI Cards (5 métriques en temps réel)

| KPI | Formule | Couleur |
|-----|---------|---------|
| Loss Ratio (S/P) | sinistres / primes | vert < 70%, orange 70–85%, rouge > 85% |
| Primes acquises | Σ earned_premium | bleu |
| Charge sinistres | Σ incurred_losses | orange |
| Fréquence sinistres | sinistres / polices | violet |
| Taux IBNR | sinistres IBNR / total | vert |

#### Graphiques

**1. S/P par branche (barres horizontales)**
- Barre colorée selon le niveau de risque (vert → rouge)
- Ligne pointillée à 75% (objectif marché)

**2. Fréquence sinistres par région (barres verticales)**
- Permet d'identifier les zones géographiques sur-sinistréeset d'ajuster la tarification

**3. Évolution du S/P par année et par branche (courbe multi-lignes)**
- Visualise la tendance de la sinistralité sur 4 ans

**4. Tableau d'alertes automatiques**
- Segments avec S/P > 85%, triés par gravité décroissante
- Affiché uniquement s'il existe des alertes actives

#### Cache et performance

```python
@st.cache_data(ttl=300)  # Cache 5 minutes
def load_mart(table: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(f"SELECT * FROM {table}").df()
    con.close()
    return df
```

Le cache évite de relire DuckDB à chaque interaction utilisateur.

### Lancement

```bash
streamlit run dashboard/app.py
# Accessible sur http://localhost:8501
```

---

## 8. Étape 6 — Orchestration Airflow

**Fichier :** `airflow_dags/insurance_pipeline_dag.py`

### DAG quotidien

```
insurance_pipeline_dag  [schedule: 0 6 * * *]  (6h00 UTC)

[generate_data] → [load_to_duckdb] → [dbt_run_staging] → [dbt_run_marts] → [dbt_test] → [notify]
```

Chaque étape est une `PythonOperator` ou `BashOperator` qui exécute la couche correspondante.

**Gestion des erreurs :**
- Retry automatique × 3 avec délai exponentiel
- Alerte email en cas d'échec du test dbt
- `on_failure_callback` pour notifier le tableau de bord

### Lancement avec Docker Compose

```bash
docker-compose up -d
# Services démarrés :
#   airflow-webserver  → http://localhost:8080
#   airflow-scheduler
#   duckdb-volume      (volume partagé)
#   streamlit          → http://localhost:8501
```

---

## 9. Glossaire actuariel

| Terme | Définition |
|-------|-----------|
| **S/P (Loss Ratio)** | Ratio Sinistres / Primes. Indicateur central de rentabilité d'un portefeuille IARD |
| **IBNR** | Incurred But Not Reported — sinistres survenus mais non encore déclarés. Fait l'objet d'une provision réglementaire |
| **Burning Cost** | S/P observé sur une période glissante, utilisé comme base de tarification |
| **Fréquence** | Nombre de sinistres / nombre de polices exposées |
| **Sévérité** | Coût moyen par sinistre |
| **Excess of Loss (XL)** | Traité de réassurance non-proportionnel : le réassureur intervient au-delà d'une rétention fixe |
| **Quota Share** | Traité de réassurance proportionnel : cession d'un % fixe de chaque risque |
| **LOB** | Line of Business — branche d'assurance (Auto, Habitation, RC, Santé…) |
