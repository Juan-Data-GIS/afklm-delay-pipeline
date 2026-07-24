import streamlit as st
import requests
import pandas as pd
import os

API_BASE_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000")

st.set_page_config(page_title="DataOps Monitoring", layout="wide")

# --- CHARGEMENT PARTAGÉ DU CSS ---
def load_global_css():
    try:
        with open("dashboard_afklm.css") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        try:
            with open("../dashboard_afklm.css") as f:
                st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
        except FileNotFoundError:
            pass

load_global_css()

st.markdown("### Rapport de Traitement de Données")
st.caption("Suivi des indicateurs de santé de la pipeline.")

if st.button("Rafraîchir - SQL", type="secondary"):
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
                st.metric(label="Volume d'Événements Ingérés", value=f"{metrics.get('total_events', 0)}")
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