# streamlit/dashboard_afklm.py
import streamlit as st

st.set_page_config(page_title="AFKLM Delay Pipeline - Control Center & ML", layout="wide")

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
URL_LIORA = "https://learn.datascientest.com/"
URL_SLIDES = "https://docs.google.com/presentation/d/1zrhrSkGNdkNAGvCf629LW4FBGnw0RLpC2AfKAt2dpMw/edit?usp=sharing"
URL_OBSERVABILITE  = "http://localhost:8501/Observabilite"
URL_AGREGATION  = "http://localhost:8501/Agregations"
URL_PREDICTION  = "http://localhost:8501/Prediction_vol"
# --- SIDEBAR FIXE  ---
with st.sidebar:
    
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
    st.link_button("Slides de présentation", url=URL_SLIDES, type="primary", use_container_width=True )


# --- CONTENU DE LA PAGE DE GARDE ---
st.markdown(
    """
    <div class="main-header">
        <h1 class="header-title">AFKLM Delay Pipeline</h1>
        <h2 class="header-subtitle">Développement d’un pipeline ETL de prédiction de retard de vol à partir d’une API publique</h2>
    </div>
    """, 
    unsafe_allow_html=True
)

st.divider()


st.markdown("""#### Accès aux fonctionnalités
Cette application permet d'accèder aux résultats d'un pipeline complet allant de l'ingestion de données de vol du groupe AirFrance-KLM à la prédiction de retards sur des vols futurs.
""")

c1, c2, c3 = st.columns(3)
with c1: 
    st.link_button("Observabilité", url=URL_OBSERVABILITE, type="primary", use_container_width=True )
    st.markdown("Suivi des indicateurs de santé de la pipeline.")
with c2: 
    st.link_button("Agrégation, accès aux métriques", url=URL_AGREGATION, type="primary", use_container_width=True )    
    st.markdown("Analyses des retards constatés")
with c3: 
    st.link_button("Prédiction", url=URL_PREDICTION, type="primary", use_container_width=True )
    st.markdown("Prédiction de retard sur les vols futurs")

st.divider()

st.markdown("#### Equipe Technique")
c1, c2, c3 = st.columns(3)

with c1:
    st.markdown('<div class="learner-card"><h5>Juan Montenegro</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)
with c2:
    st.markdown('<div class="learner-card"><h5>Pierre Foulquié</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)
with c3:
    st.markdown('<div class="learner-card"><h5>Julien Flactif</h5><p>Data Engineer Learner</p></div>', unsafe_allow_html=True)

st.divider()




