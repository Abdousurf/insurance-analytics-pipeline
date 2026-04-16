"""Dashboard d'analytique assurance.

Un outil de reporting visuel qui affiche les indicateurs clés et les graphiques
de l'assurance. Il récupère les données de notre base locale et les affiche
dans un navigateur web via Streamlit.

Exemple :
    Lancer le dashboard avec Streamlit ::

        $ streamlit run dashboard/app.py
"""

# ───────────────────────────────────────────────────────
# CE QUE FAIT CE FICHIER :
#
# Ce fichier crée un dashboard web (une page interactive) qui
# affiche les indicateurs de performance assurance et des graphiques.
# Il se connecte à notre base de données locale, récupère les données
# de synthèse et affiche :
#   - Les indicateurs clés de performance (ratio S/P, total des primes)
#   - Des graphiques en barres comparant les branches et les régions
#   - Une courbe de tendance montrant l'évolution du ratio S/P
#   - Un tableau d'alertes signalant les segments déficitaires
#
# Tout membre de l'équipe peut ouvrir cette page dans un navigateur
# pour vérifier la performance du portefeuille assurance.
# ───────────────────────────────────────────────────────

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# Emplacement du fichier de base de données locale sur le disque
DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.duckdb"

# Configurer le titre de l'onglet navigateur et la mise en page
st.set_page_config(
    page_title="Analytique Assurance",
    page_icon="📊",
    layout="wide",
)


# Ce décorateur indique à Streamlit de mémoriser (mettre en cache) les données pendant 5 minutes
# pour ne pas relire la base de données à chaque interaction
@st.cache_data(ttl=300)
def load_mart(table: str) -> pd.DataFrame:
    """Récupérer toutes les données d'une table de synthèse dans notre base.

    Ouvre une connexion en lecture seule à notre base locale, récupère toutes
    les lignes de la table demandée et les retourne sous forme de tableau.

    Args:
        table: Le nom de la table de synthèse dont récupérer les données.

    Returns:
        Un tableau (DataFrame) contenant toutes les lignes de cette table.
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(f"SELECT * FROM {table}").df()
    con.close()
    return df


def kpi_card(col, title: str, value: str, delta: str = None, color: str = "#1f77b4"):
    """Afficher une carte d'indicateur mise en valeur sur le dashboard.

    Crée un encadré coloré montrant une métrique clé (ex. « Ratio S/P : 72 % »).
    Ces cartes apparaissent en ligne en haut du dashboard pour que l'utilisateur
    voie les chiffres les plus importants d'un coup d'oeil.

    Args:
        col: La colonne du dashboard où cette carte doit apparaître.
        title: Le libellé affiché au-dessus du chiffre (ex. « Ratio S/P »).
        value: Le chiffre formaté à afficher (ex. « 72,3 % »).
        delta: Un texte optionnel affiché sous le chiffre.
        color: La couleur d'accent pour la bordure gauche et le fond de la carte.
    """
    col.markdown(
        f"""
    <div style="background:{color}15; border-left:4px solid {color};
                padding:16px; border-radius:8px; margin-bottom:8px">
        <div style="font-size:12px; color:#666; margin-bottom:4px">{title}</div>
        <div style="font-size:28px; font-weight:700; color:#222">{value}</div>
        {"<div style='font-size:12px; color:#888'>"+delta+"</div>" if delta else ""}
    </div>
    """,
        unsafe_allow_html=True,
    )


# ── En-tête ──────────────────────────────────────────────────────────────
# Afficher le titre principal et le sous-titre en haut de la page
st.title("📊 Analytique Sinistres Assurance")
st.caption("Portefeuille IARD — Pipeline d'Analytics Engineering de bout en bout")

# ── Filtres dans la barre latérale ────────────────────────────────────────
# La barre latérale à gauche permet à l'utilisateur de filtrer les données affichées
st.sidebar.header("Filtres")

# Charger la table de synthèse principale depuis la base
df = load_mart("mart_loss_ratio")

# Construire les options des menus déroulants à partir des valeurs réelles
lob_options = ["Toutes"] + sorted(df["line_of_business"].unique().tolist())
year_options = sorted(df["accident_year"].unique().tolist(), reverse=True)

# Créer les contrôles de filtre : un menu déroulant pour la branche, une sélection
# multiple pour les années, et un bouton bascule pour les segments à fort S/P
selected_lob = st.sidebar.selectbox("Branche", lob_options)
selected_year = st.sidebar.multiselect(
    "Année de survenance", year_options, default=year_options[:2]
)
show_alerts = st.sidebar.toggle("Afficher uniquement les alertes S/P élevé", value=False)

# ── Appliquer les filtres ─────────────────────────────────────────────────
# Partir du jeu de données complet et le restreindre selon les choix de l'utilisateur
filtered = df.copy()
if selected_lob != "Toutes":
    filtered = filtered[filtered["line_of_business"] == selected_lob]
if selected_year:
    filtered = filtered[filtered["accident_year"].isin(selected_year)]
if show_alerts:
    filtered = filtered[filtered["high_loss_ratio_flag"]]

# ── Ligne de KPIs ────────────────────────────────────────────────────────
# Afficher une ligne de cinq indicateurs clés en haut du dashboard
st.subheader("KPIs du portefeuille")
k1, k2, k3, k4, k5 = st.columns(5)

# Calculer les indicateurs clés à partir des données filtrées
total_premium = filtered["earned_premium_eur"].sum()
total_incurred = filtered["incurred_losses_eur"].sum()
# Ratio S/P = combien nous avons payé en sinistres par rapport aux primes encaissées
overall_lr = total_incurred / total_premium if total_premium > 0 else 0
total_claims = filtered["claim_count"].sum()
total_policies = filtered["policy_count"].sum()
# Fréquence sinistres = pourcentage de polices ayant eu un sinistre
overall_freq = total_claims / total_policies if total_policies > 0 else 0
# Sévérité moyenne = coût moyen d'un sinistre
avg_sev = total_incurred / total_claims if total_claims > 0 else 0
# IBNR = sinistres survenus mais non encore déclarés
ibnr_claims = filtered["ibnr_claims_count"].sum()
ibnr_rate = ibnr_claims / total_claims if total_claims > 0 else 0

# Colorer la carte du ratio S/P en rouge/jaune/vert selon le niveau
lr_color = (
    "#e74c3c" if overall_lr > 0.85 else "#27ae60" if overall_lr < 0.70 else "#f39c12"
)

# Afficher chaque métrique dans sa propre carte
kpi_card(k1, "Ratio S/P", f"{overall_lr:.1%}", color=lr_color)
kpi_card(k2, "Primes acquises", f"€{total_premium/1e6:.1f}M", color="#3498db")
kpi_card(k3, "Charge sinistres", f"€{total_incurred/1e6:.1f}M", color="#e67e22")
kpi_card(k4, "Fréquence sinistres", f"{overall_freq:.2%}", color="#9b59b6")
kpi_card(k5, "Taux IBNR", f"{ibnr_rate:.1%}", color="#1abc9c")

st.divider()

# ── Graphiques ────────────────────────────────────────────────────────────
# Afficher deux graphiques côte à côte
col1, col2 = st.columns(2)

# Graphique de gauche : barres horizontales comparant les ratios S/P par branche
with col1:
    st.subheader("Ratio S/P par branche")
    # Regrouper les données par branche et calculer le ratio S/P de chacune
    lob_agg = (
        filtered.groupby("line_of_business")
        .agg(
            earned=("earned_premium_eur", "sum"),
            incurred=("incurred_losses_eur", "sum"),
        )
        .assign(loss_ratio=lambda d: d["incurred"] / d["earned"])
        .reset_index()
        .sort_values("loss_ratio", ascending=True)
    )
    fig = px.bar(
        lob_agg,
        x="loss_ratio",
        y="line_of_business",
        orientation="h",
        text_auto=".1%",
        color="loss_ratio",
        color_continuous_scale=["#27ae60", "#f39c12", "#e74c3c"],
        range_color=[0.5, 1.0],
    )
    # Ajouter une ligne verticale en pointillés indiquant l'objectif de 75 %
    fig.add_vline(
        x=0.75, line_dash="dash", line_color="gray", annotation_text="Objectif 75 %"
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

# Graphique de droite : barres verticales montrant la fréquence sinistres par région
with col2:
    st.subheader("Fréquence sinistres par région")
    # Regrouper les données par région et calculer le taux de sinistralité
    reg_agg = (
        filtered.groupby("region")
        .agg(claims=("claim_count", "sum"), policies=("policy_count", "sum"))
        .assign(freq=lambda d: d["claims"] / d["policies"])
        .reset_index()
        .sort_values("freq", ascending=False)
    )
    fig2 = px.bar(
        reg_agg,
        x="region",
        y="freq",
        text_auto=".2%",
        color="freq",
        color_continuous_scale="RdYlGn_r",
    )
    fig2.update_layout(coloraxis_showscale=False, height=300, xaxis_tickangle=-30)
    st.plotly_chart(fig2, use_container_width=True)

# ── Tendance du ratio S/P ────────────────────────────────────────────────
# Afficher une courbe de l'évolution du ratio S/P au fil des années
st.subheader("Tendance du ratio S/P par année et branche")
# Regrouper les données non filtrées par année et branche pour voir la tendance complète
trend = (
    df.groupby(["accident_year", "line_of_business"])
    .agg(ep=("earned_premium_eur", "sum"), il=("incurred_losses_eur", "sum"))
    .assign(lr=lambda d: d["il"] / d["ep"])
    .reset_index()
)
fig3 = px.line(
    trend,
    x="accident_year",
    y="lr",
    color="line_of_business",
    markers=True,
    labels={"lr": "Ratio S/P", "accident_year": "Année"},
)
# Ajouter une ligne horizontale en pointillés à 75 % pour indiquer l'objectif
fig3.add_hline(y=0.75, line_dash="dot", line_color="gray", annotation_text="Objectif")
fig3.update_yaxes(tickformat=".0%")
st.plotly_chart(fig3, use_container_width=True)

# ── Tableau d'alertes ─────────────────────────────────────────────────────
# Afficher un tableau d'avertissement pour les segments où le ratio S/P est dangereusement élevé
alerts = df[df["high_loss_ratio_flag"]].sort_values("loss_ratio", ascending=False)
if not alerts.empty:
    st.subheader(f"⚠️ Alertes ratio S/P élevé ({len(alerts)} segments)")
    st.dataframe(
        alerts[
            [
                "line_of_business",
                "region",
                "accident_year",
                "loss_ratio",
                "earned_premium_eur",
                "claim_count",
            ]
        ].style.format(
            {
                "loss_ratio": "{:.1%}",
                "earned_premium_eur": "€{:,.0f}",
            }
        ),
        use_container_width=True,
    )
