import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# Configuration de la page
st.set_page_config(page_title="DST Airlines - Control Center & ML", layout="wide")

API_BASE_URL = "http://fastapi:8000"

# --- HEADER ET METADONNÉES ---
head_col1, head_col2 = st.columns([2.8, 1.2])

with head_col1:
    st.markdown(
        """
        <h1 style='display: flex; align-items: center;'>
            <img src='https://img.icons8.com/color/96/000000/control-panel.png' width='50' style='margin-right: 15px;'>
            DST Airlines : Control Center & ML
        </h1>
        """, 
        unsafe_allow_html=True
    )
    st.subheader("Observabilité du Pipeline & Analyses Prédictives ML")
    st.markdown("Dashboard unifié alimenté par l'API FastAPI de production.")

with head_col2:
    logo_sub_col1, logo_sub_col2 = st.columns([1, 1.5], vertical_alignment="center")
    with logo_sub_col1:
        st.image("https://s3-eu-west-1.amazonaws.com/tpd/logos/697a305f794e2f0e63fba37b/0x0.png", width=70)
    with logo_sub_col2:
        st.image("https://logo-marque.com/wp-content/uploads/2020/03/Air-France-Logo.png", width=140)
    
    st.markdown(
        """
        <div style="background-color: #f0f2f6; padding: 12px; border-radius: 10px; border-left: 5px solid #002244; margin-top: 10px;">
            <p style="margin: 0; font-weight: bold; color: #002244;">🚀 Cursus : Data Engineer</p>
            <p style="margin: 0; font-size: 0.85em;">• Pierre Foulquier | Juan Montenegro | Julien Flactif</p>
        </div>
        """,
        unsafe_allow_html=True
    )

st.divider()

# --- SÉPARATION EN DEUX ONGLETS MAJEURS (SOBRE & DATA ENG) ---
tab_ops, tab_ml = st.tabs([" Observabilité Pipeline (Data Ops)", "🤖 Analyses Prédictives (Machine Learning)"])

# ==========================================
# ONGLET 1 : OBSERVABILITÉ (LOGS D'ORCHESTRATION)
# ==========================================
with tab_ops:
    st.markdown(
        """
        <h3 style='display: flex; align-items: center;'>
            <img src='https://img.icons8.com/color/48/000000/settings.png' width='30' style='margin-right: 10px;'>
            Pipeline Orchestration Logs (logs.airflow_events)
        </h3>
        """, unsafe_allow_html=True
    )

    # Bouton optionnel : vider le cache Streamlit si on veut forcer l'appel API
    if st.button("Rafraîchir les logs d'orchestration", type="secondary"):
        st.cache_data.clear()

    try:
        # Appel API automatique au chargement de l'onglet
        response = requests.get(f"{API_BASE_URL}/v1/monitoring/pipeline-logs", timeout=5)
        
        if response.status_code == 200:
            logs_data = response.json()
            
            if logs_data:
                # ÉTAPE CLÉ : On définit proprement df_logs ici
                df_logs = pd.DataFrame(logs_data)
                
                # Calcul de KPIs rapides pour le jury
                success_count = df_logs['event_type'].str.contains('success', case=False, na=False).sum()
                total_logs = len(df_logs)
                
                kpi_ops1, kpi_ops2 = st.columns(2)
                kpi_ops1.metric("Derniers événements analysés", total_logs)
                kpi_ops2.metric("Statuts Success observés", f"{success_count} / {total_logs}")
                
                st.markdown("#### 10 derniers événements du pipeline :")
                # Affichage sécurisé du tableau
                st.dataframe(
                    df_logs[["event_at", "level", "layer", "dag_id", "task_id", "event_type", "message"]].head(10),
                    use_container_width=True
                )
            else:
                st.info("Aucun log trouvé dans la table logs.airflow_events.")
        else:
            st.error(" L'API FastAPI est fonctionnelle, mais renvoie une erreur lors de l'accès à Supabase.")
            with st.expander("Détails techniques"):
                st.write(response.text)
                
    except requests.exceptions.ConnectionError:
        st.error(" Le conteneur 'fastapi' est inaccessible ou hors ligne dans le réseau Docker.")
    except Exception as e:
        st.error(f" Erreur lors de l'affichage des données : {e}")
# ==========================================
# ONGLET 2 : ANALYTICS MACHINE LEARNING
# ==========================================
with tab_ml:
    st.markdown(
        """
        <h3 style='display: flex; align-items: center;'>
            <img src='https://img.icons8.com/color/48/000000/brainstorm_skill.png' width='30' style='margin-right: 10px;'>
            Exploration des prédictions de retards
        </h3>
        """, unsafe_allow_html=True
    )
    
    # 1. Menu de sélection des paramètres pour le jury
    dimension = st.selectbox(
        "Choisissez l'axe d'analyse pour calculer le taux de retard :",
        options=["airport", "city", "airline", "date"],
        format_func=lambda x: {"airport": "Aéroport de départ", "city": "Ville de départ", "airline": "Compagnie aérienne", "date": "Date du vol"}[x]
    )
    
    # 2. Bouton de validation pour déclencher le calcul de l'API
    if st.button("Calculer les indicateurs de scoring", type="primary"):
        with st.spinner("Calcul des agrégats ML en cours via FastAPI..."):
            try:
                # Requête sur le endpoint dynamique
                metrics_res = requests.get(f"{API_BASE_URL}/v1/analytics/ml-metrics", params={"dimension": dimension}, timeout=15)
                matrix_res = requests.get(f"{API_BASE_URL}/v1/analytics/confusion-matrix", timeout=15)
                
                if metrics_res.status_code == 200 and matrix_res.status_code == 200:
                    df_metrics = pd.DataFrame(metrics_res.json())
                    matrix_data = matrix_res.json()
                    
                    st.divider()
                    
                    # --- AFFICHAGE DE LA MATRICE DE PERFORMANCE (KPIs) ---
                    st.markdown("####  Qualité globale des prédictions du modèle (Matrice de Confusion)")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Vrais Positifs (VP)", f"{matrix_data.get('Vrais Positifs (VP)', 0):,}", help="Retards prédits et réellement arrivés")
                    c2.metric("Vrais Négatifs (VN)", f"{matrix_data.get('Vrais Négatifs (VN)', 0):,}", help="Vols à l'heure prédits et réellement à l'heure")
                    c3.metric("Faux Positifs (FP)", f"{matrix_data.get('Faux Positifs (FP)', 0):,}", help="Modèle a prédit un retard mais le vol était à l'heure")
                    c4.metric("Faux Négatifs (FN)", f"{matrix_data.get('Faux Négatifs (FN)', 0):,}", help="Modèle a prédit à l'heure mais le vol était en retard")
                    
                    st.divider()
                    
                    # --- AFFICHAGE DU GRAPHIQUE ANALYTIQUE ---
                    st.markdown(f"####  Taux de retard prédit par *{dimension}*")
                    
                    # Graphique Plotly Express propre
                    fig = px.bar(
                        df_metrics.head(15), # On limite au top 15 pour la lisibilité
                        x="label", 
                        y="delayed_share",
                        labels={"label": "Élément", "delayed_share": "% de retard prédit"},
                        color="delayed_share",
                        color_continuous_scale="Reds"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Tableau brut en dessous
                    st.markdown("Données détaillées :")
                    st.dataframe(df_metrics, use_container_width=True)
                    
                else:
                    st.error("Erreur lors du calcul des données de scoring par l'API.")
            except Exception as e:
                st.error(f"Erreur lors de l'appel API : {e}")
    else:
        st.info("💡 Sélectionnez un paramètre ci-dessus et validez pour afficher les résultats du Machine Learning.")