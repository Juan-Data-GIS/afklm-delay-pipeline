# streamlit/dashboard_afklm.py
import streamlit as st

st.set_page_config(page_title="DST Airlines - Control Center & ML", layout="wide")

def local_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass

local_css("dashboard_afklm.css")

# --- CARTOGRAPHIE DES URLS ---
URL_GITHUB_PROJET = "https://github.com/Juan-Data-GIS/afklm-delay-pipeline"
URL_FASTAPI_DOCS = "http://localhost:8000/docs"
URL_AIRFLOW = "http://localhost:8081"
URL_PROMETHEUS = "http://localhost:9090"
URL_GRAFANA = "http://localhost:3000"
URL_LIORA = "https://learn.datascientest.com/lessons"

# --- SIDEBAR FIXE  ---
with st.sidebar:
    st.image("https://logo-marque.com/wp-content/uploads/2020/03/Air-France-Logo.png", width=160)
    st.divider()
    
    # Section Liens Externes demandée
    st.markdown("### Liens Externes")
    st.link_button("Depot GitHub Projet", url=URL_GITHUB_PROJET, type="primary", use_container_width=True)
    st.link_button("Portail Liora", url=URL_LIORA, type="secondary", use_container_width=True)
    
    st.divider()
    st.markdown("### Ecosysteme Technique")
    st.link_button("Orchestrateur Airflow 3", url=URL_AIRFLOW, type="secondary", use_container_width=True)
    st.link_button("Interface Grafana", url=URL_GRAFANA, type="secondary", use_container_width=True)
    st.link_button("Documentation FastAPI", url=URL_FASTAPI_DOCS, type="secondary", use_container_width=True)
    st.link_button("Metriques Prometheus", url=URL_PROMETHEUS, type="secondary", use_container_width=True)
    st.divider()
    st.markdown(
        """
        <div class="api-status-container">
            <strong>Target Node :</strong> <code>Local Sandbox (Docker)</code>
        </div>
        """, 
        unsafe_allow_html=True
    )

# --- CONTENU DE LA PAGE DE GARDE ---
st.markdown(
    """
    <div class="main-header">
        <h1 class="header-title">Soutenance de Projet - Fin de Cycle</h1>
        <h2 class="header-subtitle">Plateforme unifiee d'Ingestion, de Monitoring et de ML(AFKLM)</h2>
    </div>
    """, 
    unsafe_allow_html=True
)

st.divider()

col_logo1, col_logo2 = st.columns([1, 1])
with col_logo1:
    st.image("https://logo-marque.com/wp-content/uploads/2020/03/Air-France-Logo.png", width=200)
with col_logo2:
    st.image("https://s3-eu-west-1.amazonaws.com/tpd/logos/697a305f794e2f0e63fba37b/0x0.png", width=75)
    st.caption("Partenaire Metier : Liora")
    
st.markdown("<br>", unsafe_allow_html=True)

# Renommage de la section apprenants demandé
st.markdown("#### Equipe Technique (Data Engineer en formation)")
c1, c2, c3 = st.columns(3)

with c1:
    st.markdown('<div class="learner-card"><h5>Juan Montenegro</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)
with c2:
    st.markdown('<div class="learner-card"><h5>Pierre Foulquié</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)
with c3:
    st.markdown('<div class="learner-card"><h5>Julien Flactif</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)

st.divider()

st.markdown(
    """
    ### Perimetre Technique Valide
    L'objectif de cette soutenance est de valider le fonctionnement d'un pipeline complet d'ingestion de données de vol de la compagnie Air France-KLM, adossé à un cas d'usage analytique de prédiction de retards (MLOps).
    
    * **Pipeline DataOps :** Ingestion et orchestration multi-couches gérées de bout en bout par **Apache Airflow 3**.
    * **Persistance :** Instance relationnelle PostgreSQL hébergée en local sur l'environnement d'évaluation.
    * **Service d'Exposition :** Développement d'une API de production sous **FastAPI** assurant le requêtage dynamique et l'exposition des métriques d'infrastructure.
    * **Supervision & Telemetrie :** Collecte des données de santé système via **Prometheus** et restitution graphique centralisée sur **Grafana**.
    """
)