# streamlit/Accueil.py
import streamlit as st

from sidebar_common import render_sidebar

st.set_page_config(page_title="AFKLM Delay Pipeline - Control Center & ML", layout="wide")

def local_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass

local_css("dashboard_afklm.css")

render_sidebar()

# --- CONTENU DE LA PAGE DE GARDE ---
col_logo_af, col_center, col_logo_liora = st.columns([1, 4, 1])
with col_logo_af:
    st.image("https://logo-marque.com/wp-content/uploads/2020/03/Air-France-Logo.png", width=150)
with col_center:
    st.markdown(
        """
        <div class="main-header">
            <h1 class="header-title">AFKLM Delay Pipeline</h1>
            <h2 class="header-subtitle">Développement d’un pipeline ETL de prédiction de retard de vol à partir d’une API publique</h2>
        </div>
        """,
        unsafe_allow_html=True
    )
with col_logo_liora:
    st.image("https://s3-eu-west-1.amazonaws.com/tpd/logos/697a305f794e2f0e63fba37b/0x0.png", width=75)

st.markdown(
    """
    <div class="project-description">
    Ce projet répond à une problématique concrète du transport aérien : <strong>anticiper les retards de vol</strong>.
    À partir des données publiques diffusées par le groupe Air France-KLM, nous avons construit un pipeline complet,
    de l'ingestion à la prédiction.
    </div>
    """,
    unsafe_allow_html=True
)

st.divider()

st.markdown("### Equipe Technique")
c1, c2, c3 = st.columns(3)

with c1:
    st.markdown('<div class="learner-card"><h5>Juan Montenegro</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)
with c2:
    st.markdown('<div class="learner-card"><h5>Pierre Foulquié</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)
with c3:
    st.markdown('<div class="learner-card"><h5>Julien Flactif</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)

st.divider()

st.markdown("### Stack Technique")
st.caption("Pipeline orchestré par Apache Airflow, monitoré via Prometheus + Grafana, persisté sur PostgreSQL.")

STACK = [
    ("assets/logos/air_france.svg", "Air France API",         "Source des données"),
    ("assets/logos/dlt.png",        "dlt",                    "Ingestion"),
    ("assets/logos/dbt.svg",        "dbt",                    "Transformation SQL"),
    ("assets/logos/xgboost.png",    "XGBoost + scikit-learn", "Prédiction de retard"),
    ("assets/logos/fastapi.png",    "FastAPI",                "Exposition REST"),
    ("assets/logos/streamlit.png",  "Streamlit",              "Dashboard"),
]

stack_cols = st.columns(len(STACK))
for col, (logo_path, name, role) in zip(stack_cols, STACK):
    with col:
        st.image(logo_path, width=80)
        st.markdown(f"**{name}**")
        st.caption(role)
