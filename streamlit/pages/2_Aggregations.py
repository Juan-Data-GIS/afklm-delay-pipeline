import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os

API_BASE_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000")

st.set_page_config(page_title="Analyses des retards constatés", layout="wide")

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

st.markdown("### Analyse des retards")
st.caption("Agrégation des retards par segmentation.")

with st.container(border=True):
    st.markdown("**Paramétrage de la requête**")
    
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

    
    with st.spinner("Calcul des agrégations..."):
        try:
            url_ml = f"{API_BASE_URL}/v1/analytics/delay-metrics?dimension={dimension_query}"
            response = requests.get(url_ml, timeout=10)
            
            if response.status_code == 200:
                raw_data = response.json()
                
                kpis = None
                breakdown_data = []
                
                if isinstance(raw_data, dict):
                    kpis = raw_data.get("kpis")
                    breakdown_data = raw_data.get("breakdown", [])
                elif isinstance(raw_data, list):
                    breakdown_data = raw_data
                
                if breakdown_data:
                    df_ml = pd.DataFrame(breakdown_data)
                    df_ml = df_ml.sort_values(by="delayed_share", ascending=False).reset_index(drop=True)
                    
                    # --- AFFICHAGE DES KPIS EN CARTES (Si disponibles) ---
                    if kpis:
                        st.markdown("#### Indicateurs de Performance Globaux (Axe filtré)")
                        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
                        with kpi_col1:
                            st.metric(label="Taux de retard moyen", value=f"{kpis.get('global_rate')}%")
                        with kpi_col2:
                            st.metric(label="Volume de vols enregistrés", value=f"{kpis.get('total_vols'):,}".replace(",", " "))
                        with kpi_col3:
                            st.metric(label="Total retards enregistrés", value=f"{kpis.get('total_retards'):,}".replace(",", " "))
                        st.divider()
                    
                    # Message d'analyse discriminante
                    top_1 = df_ml.iloc[0]
                    st.info(f"Analyse discriminante : La catégorie **{top_1['label']}** présente le taux de retard le plus élevé ({top_1['delayed_share']}%).")
                    
                    # Filtrage du volume d'affichage
                    if top_n == "Top 5":
                        df_chart = df_ml.head(5)
                    elif top_n == "Top 10":
                        df_chart = df_ml.head(10)
                    elif top_n == "Top 20":
                        df_chart = df_ml.head(20)
                    else:
                        df_chart = df_ml
                    
                    # Construction du graphique
                    fig = px.bar(
                        df_chart, x='label', y='delayed_share',
                        title=f"Distribution du Taux de Retard (%) par {dimension_label} ({top_n})",
                        labels={'label': dimension_label, 'delayed_share': 'Taux de retard (%)'},
                        color='delayed_share', 
                        color_continuous_scale="blues"
                    )
                    fig.update_layout(template="plotly_white")
                    
                    # --- AJOUT DE LA LIGNE DE MOYENNE HORIZONTALE ---
                    if kpis and "global_rate" in kpis:
                        moyenne_globale = kpis.get("global_rate", 0)
                        fig.add_hline(
                            y=moyenne_globale, 
                            line_dash="dash", 
                            line_color="red",
                            annotation_text=f"Moyenne globale : {moyenne_globale}%", 
                            annotation_position="top left"
                        )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Aucun enregistrement trouvé dans le registre de scoring.")
            else:
                st.error(f"Erreur API ({response.status_code}) lors de la récupération des métriques.")
        except Exception as e:
            st.error(f"Exception levée lors de l'appel API : {e}")