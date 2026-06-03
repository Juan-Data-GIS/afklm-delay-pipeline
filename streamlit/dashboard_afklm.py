import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- ARCHITECTURE DATAOPS : IMPORT DU SYSTEME DE LOGS PARTAGE ---
from utils.monitoring_utils import log_event

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="DST Airlines - Control Center & ML", layout="wide")

# --- BEST PRACTICE DEVELOPPEMENT : CHARGEMENT DU CSS ALIGNÉ ---
def local_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass

# Chargement de la feuille de style externe
local_css("dashboard_afklm.css")

# --- CARTOGRAPHIE DES URLS INFRASTRUCTURE (LOCAL DOCKER) ---
API_BASE_URL = "http://fastapi:8000"
URL_FASTAPI_DOCS = "http://localhost:8000/docs"
URL_AIRFLOW = "http://localhost:8081"
URL_PROMETHEUS = "http://localhost:9090"
URL_GRAFANA = "http://localhost:3000"

# ==============================================================================
# BARRE LATÉRALE (SIDEBAR) & ECOSYSTÈME TECHNIQUE
# ==============================================================================
with st.sidebar:
    st.image("https://logo-marque.com/wp-content/uploads/2020/03/Air-France-Logo.png", width=160)
    
    st.markdown("### 🏢 Navigation")
    page = st.radio(
        "Sélection :",
        [
            "🏠 Accueil & Soutenance", 
            "📊 Observabilité Pipeline (Data Ops)", 
            "🤖 Analyses Prédictives (ML)"
        ]
    )
    
    st.divider()
    
    st.markdown("### 🛠️ Écosystème Technique")
    st.link_button("📈 Interface Grafana", url=URL_GRAFANA, type="secondary", use_container_width=True)
    st.link_button("🌪️ Orchestrateur Airflow 3", url=URL_AIRFLOW, type="secondary", use_container_width=True)
    st.link_button("🔌 Documentation FastAPI", url=URL_FASTAPI_DOCS, type="secondary", use_container_width=True)
    st.link_button("🔥 Métriques Prometheus", url=URL_PROMETHEUS, type="secondary", use_container_width=True)
    
    st.markdown(
        """
        <div class="api-status-container">
            <strong>API Status :</strong> <span class="status-connected">● Connected</span><br>
            <strong>Target Node :</strong> <code>Local Sandbox (Docker)</code>
        </div>
        """, 
        unsafe_allow_html=True
    )

# ==============================================================================
# PAGE 1 : PAGE DE GARDE & PRÉSENTATION (SOUTENANCE)
# ==============================================================================
if page == "🏠 Accueil & Soutenance":
    
    st.markdown(
        """
        <div class="main-header">
            <h1 class="header-title">Soutenance de Projet - Fin de Cycle</h1>
            <h2 class="header-subtitle">Plateforme unifiée d'Ingestion, de Monitoring et de MLOps (AFKLM Data Hub)</h2>
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
        st.caption("Partenaire Métier : Liora")
        
    st.markdown("<br>", unsafe_allow_html=True)
    
    st.markdown("#### 👥 Candidats au Titre (Data Engineer Learners)")
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
        ### 📋 Périmètre Technique Validé
        L'objectif de cette soutenance est de valider le fonctionnement d'un pipeline complet d'ingestion de données de vol de la compagnie Air France-KLM, adossé à un cas d'usage analytique de prédiction de retards (MLOps).
        
        * **Pipeline DataOps :** Ingestion et orchestration multi-couches gérées de bout en bout par **Apache Airflow 3**.
        * **Persistance :** Instance relationnelle PostgreSQL hébergée en local sur l'environnement d'évaluation.
        * **Service d'Exposition :** Développement d'une API de production sous **FastAPI** assurant le requêtage dynamique et l'exposition des métriques d'infrastructure.
        * **Supervision & Télémétrie :** Collecte des données de santé système via **Prometheus** et restitution graphique centralisée sur **Grafana**.
        """
    )

# ==============================================================================
# PAGE 2 : OBSERVABILITÉ PIPELINE (DATA OPS)
# ==============================================================================
elif page == "📊 Observabilité Pipeline (Data Ops)":
    st.markdown("### 📊 Rapport de Traitement de Données (DataOps)")
    st.caption("Suivi des indicateurs de santé et intégrité de l'orchestration.")

    if st.button("Rafraîchir la télémétrie SQL", type="secondary"):
        st.cache_data.clear()

    try:
        response = requests.get(f"{API_BASE_URL}/v1/monitoring/pipeline-logs", timeout=5)
        if response.status_code == 200:
            data_payload = response.json()
            metrics = data_payload.get("metrics", {})
            logs_list = data_payload.get("logs", [])
            
            if logs_list:
                df_logs = pd.DataFrame(logs_list)
                
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.metric(label="Fenêtre Temporelle", value=f"{metrics.get('days_covered', 1)} jours")
                with m2:
                    st.metric(label="Volume d'Événements Ingestrés", value=f"{metrics.get('total_events', 0)}")
                with m3:
                    err_rate = metrics.get('error_rate', 0.0)
                    st.metric(
                        label="Taux d'Échec Critique", 
                        value=f"{err_rate}%", 
                        delta=f"{err_rate}% Anomalies" if err_rate > 0 else "0% Anomalies",
                        delta_color="inverse" if err_rate > 0 else "normal"
                    )
                with m4:
                    st.metric(label="Lignes en Base (Flight Legs)", value=f"{metrics.get('total_vols_mart', 0):,}")
                
                st.divider()
                
                df_errors = df_logs[df_logs['level'] == 'ERROR']
                if not df_errors.empty:
                    st.error(f"Incident report : {len(df_errors)} anomalies interceptées en base.")
                    st.dataframe(df_errors[["event_at", "layer", "dag_id", "task_id", "message"]], use_container_width=True)
                else:
                    st.success("Statut d'exécution nominal : Aucun blocage recensé sur le cycle courant.")

                st.markdown("#### Registre des Dernières Exécutions (Logs)")
                st.dataframe(df_logs[["event_at", "level", "layer", "dag_id", "task_id", "event_type", "message"]].head(15), use_container_width=True)
        else:
            st.error("Défaut de réponse de la passerelle API (FastAPI backend).")
    except Exception as e:
        st.error(f"Erreur d'acquisition réseau : {e}")

# ==============================================================================
# PAGE 3 : INTERACTION JURY & INFÉRENCE ML (AVEC FILTRE TOP N OPTIMISÉ)
# ==============================================================================
elif page == "🤖 Analyses Prédictives (ML)":
    st.markdown("### 🤖 Analyse d'Inférence Predictive (XGBoost Model)")
    st.caption("Simulation et requêtage des indicateurs de retards par axe analytique.")

    with st.container(border=True):
        st.markdown("**Paramétrage de la requête analytique (Session Jury)**")
        
        col_select1, col_select2 = st.columns([2, 1])
        
        with col_select1:
            dimension_label = st.selectbox(
                "Axe de segmentation :",
                ["Aéroport de départ", "Ville de départ", "Date de vol", "Compagnie aérienne"]
            )
            
        with col_select2:
            top_n = st.selectbox(
                "Volume d'affichage :",
                ["Top 5", "Top 10", "Top 20", "Afficher Tout"]
            )
            
        mapping_dim = {
            "Aéroport de départ": "airport",
            "Ville de départ": "city",
            "Date de vol": "date",
            "Compagnie aérienne": "airline"
        }
        dimension_query = mapping_dim[dimension_label]
        
        trigger_api = st.button("Exécuter la requête sur l'API", type="primary")

    st.divider()

    if trigger_api:
        log_event(
            level="INFO",
            layer="UI",
            dag_id="dashboard_session",
            task_id="jury_trigger_query",
            event_type="streamlit_click_query",
            message=f"Requête analytique déclenchée par l'UI sur la dimension : {dimension_label} ({top_n})."
        )
        
        with st.spinner("Calcul des agrégations MLOps..."):
            try:
                url_ml = f"{API_BASE_URL}/v1/analytics/ml-metrics?dimension={dimension_query}"
                response = requests.get(url_ml, timeout=10)
                if response.status_code == 200:
                    ml_data = response.json()
                    if ml_data:
                        df_ml = pd.DataFrame(ml_data)
                        df_ml = df_ml.sort_values(by="delayed_share", ascending=False).reset_index(drop=True)
                        
                        top_1 = df_ml.iloc[0]
                        st.info(f"Analyse discriminante : La catégorie **{top_1['label']}** présente le taux de retard théorique le plus élevé ({top_1['delayed_share']}%).")
                        
                        if top_n == "Top 5":
                            df_chart = df_ml.head(5)
                        elif top_n == "Top 10":
                            df_chart = df_ml.head(10)
                        elif top_n == "Top 20":
                            df_chart = df_ml.head(20)
                        else:
                            df_chart = df_ml
                        
                        fig = px.bar(
                            df_chart, x='label', y='delayed_share',
                            title=f"Distribution du Taux de Retard (%) par {dimension_label} ({top_n})",
                            labels={'label': dimension_label, 'delayed_share': 'Taux de retard (%)'},
                            color='delayed_share', 
                            color_continuous_scale="blues"
                        )
                        fig.update_layout(template="plotly_white")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Aucun enregistrement trouvé dans le registre de scoring.")
                else:
                    log_event(
                        level="ERROR",
                        layer="UI",
                        dag_id="dashboard_session",
                        task_id="jury_trigger_query",
                        event_type="streamlit_api_error",
                        message=f"Erreur de communication API lors de l'analyse ML : Code HTTP {response.status_code}."
                    )
                    st.error(f"Erreur API ({response.status_code}) lors de la récupération des métriques.")
            except Exception as e:
                log_event(
                    level="ERROR",
                    layer="UI",
                    dag_id="dashboard_session",
                    task_id="jury_trigger_query",
                    event_type="streamlit_network_failure",
                    message=f"Exception matérielle réseau sur l'UI Streamlit : {str(e)}."
                )
                st.error(f"Exception levée lors de l'appel API : {e}")
    else:
        st.info("Sélectionnez l'axe souhaité ci-dessus pour lancer la routine de calcul.")