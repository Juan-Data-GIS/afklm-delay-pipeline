import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import date

# Configuration de la page
st.set_page_config(page_title="DST Airlines - Control Center", layout="wide")

# --- DICTIONNAIRES DE TRADUCTION ---
AIRLINE_MAPPING = {
    "AF": "Air France",
    "KL": "KLM Royal Dutch Airlines",
    "DL": "Delta Air Lines",
    "A5": "HOP!",
    "TO": "Transavia",
    "VS": "Virgin Atlantic",
    "KQ": "Kenya Airways",
    "AM": "Aeroméxico",
    "G3": "Gol Linhas Aéreas",
    "UX": "Air Europa",
    "MU": "China Eastern Airlines",
    "CZ": "China Southern Airlines"
}

# Mapping inversé pour retrouver le code IATA à partir du nom (pour l'API)
REVERSE_MAPPING = {v: k for k, v in AIRLINE_MAPPING.items()}

# --- FONCTION DE RÉCUPÉRATION DES LOGS (DATA OPS) ---
@st.cache_data(ttl=60)
def fetch_monitoring_stats():
    """Récupère les métriques de la table logs.job_runs via FastAPI"""
    try:
        response = requests.get("http://fastapi:8000/monitoring-stats", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return {"days": 0, "total_rows": 0, "error_rate": 0, "last_status": "API Down"}

# --- HEADER ET METADONNÉES ---
head_col1, head_col2 = st.columns([2.8, 1.2])

with head_col1:
    st.markdown(
        """
        <h1 style='display: flex; align-items: center;'>
            <img src='https://img.icons8.com/color/96/000000/control-panel.png' width='50' style='margin-right: 15px;'>
            DST Airlines : Control Center
        </h1>
        """, 
        unsafe_allow_html=True
    )
    st.subheader("Monitoring en temps réel du trafic Air France-KLM")
    st.markdown("Dashboard alimenté par la couche **Silver** du Data Warehouse (Supabase).")

with head_col2:
    logo_sub_col1, logo_sub_col2 = st.columns([1, 1.5], vertical_alignment="center")
    
    with logo_sub_col1:
        st.image("https://s3-eu-west-1.amazonaws.com/tpd/logos/697a305f794e2f0e63fba37b/0x0.png", width=70)

    with logo_sub_col2:
        st.image("https://logo-marque.com/wp-content/uploads/2020/03/Air-France-Logo.png", width=140)
    
    st.markdown(
        """
        <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #002244; margin-top: 15px;">
            <p style="margin: 0; font-weight: bold; color: #002244;">🚀 Projet : DST Airlines</p>
            <p style="margin: 0; font-size: 0.9em;"><strong>Cursus :</strong> Data Engineer</p>
            <p style="margin: 0; font-size: 0.9em;"><strong>Source :</strong> <a href="https://developer.airfranceklm.com" target="_blank" style="color: #004488; text-decoration: none;">API Développeur AFKLM</a></p>
            <hr style="margin: 10px 0;">
            <p style="margin: 0; font-size: 0.85em; font-weight: bold;">Équipe :</p>
            <p style="margin: 0; font-size: 0.85em;">• Pierre Foulquier</p>
            <p style="margin: 0; font-size: 0.85em;">• Juan Montenegro</p>
            <p style="margin: 0; font-size: 0.85em;">• Julien Flactif</p>
        </div>
        """,
        unsafe_allow_html=True
    )

st.divider()

# --- NOUVELLE SECTION : MONITORING DATA OPS ---
st.markdown(
    """
    <h3 style='display: flex; align-items: center;'>
        <img src='https://img.icons8.com/color/48/000000/settings.png' width='30' style='margin-right: 10px;'>
        Data Pipeline Health (Observabilité Logs)
    </h3>
    """, unsafe_allow_html=True
)

stats_ops = fetch_monitoring_stats()
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("Jours d'ingestion", f"{stats_ops['days']} j", help="Historique disponible en base")
with m2:
    st.metric("Lignes Bronze", f"{stats_ops['total_rows']:,}", help="Volume total extrait de l'API")
with m3:
    # On inverse la couleur car un taux d'erreur élevé est "mauvais"
    st.metric("Taux d'échec API", f"{stats_ops['error_rate']}%", delta=f"{stats_ops['error_rate']}%", delta_color="inverse")
with m4:
    status_icon = "🟢" if "SUCCESS" in stats_ops['last_status'] else "🔴"
    st.metric("Statut Pipeline", f"{status_icon} {stats_ops['last_status']}")

st.divider()

# --- ZONE DE FILTRES ---
st.markdown(
    """
    <h3 style='display: flex; align-items: center;'>
        <img src='https://img.icons8.com/color/48/000000/filter--v1.png' width='30' style='margin-right: 10px;'>
        Paramètres d'analyse
    </h3>
    """, 
    unsafe_allow_html=True
)

# On place les filtres dans un conteneur stylisé
with st.container():
    col_filter1, col_filter2 = st.columns(2)
    
    with col_filter1:
        selected_date = st.date_input("Date du vol", value=date.today())
        
    with col_filter2:
        options_compagnies = ["Toutes les compagnies"] + list(AIRLINE_MAPPING.values())
        selected_airline_name = st.selectbox("Compagnie aérienne", options=options_compagnies)

# --- GESTION DE LA SESSION ET BOUTON ---
if 'data_loaded' not in st.session_state:
    st.session_state['data_loaded'] = False

# Bouton d'action
if st.button("Actualiser les données avec ces filtres", type="primary", use_container_width=True):
    st.cache_data.clear()
    st.session_state['data_loaded'] = True
    # On sauvegarde les choix pour la requête
    st.session_state['api_date'] = selected_date.strftime("%Y-%m-%d")
    st.session_state['api_airline'] = REVERSE_MAPPING.get(selected_airline_name, "ALL")

# --- RÉCUPÉRATION DES DONNÉES MÉTIER ---
@st.cache_data(ttl=60)
def fetch_data(query_date, query_airline):
    try:
        # On passe les filtres en paramètres à FastAPI (ex: ?date=2026-04-29&airline=AF)
        params = {"date": query_date, "airline": query_airline}
        response = requests.get("http://fastapi:8000/flight-stats", params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erreur de connexion à l'API : {e}")
        return None

# --- AFFICHAGE DU DASHBOARD ---
if st.session_state['data_loaded']:
    
    with st.spinner("Récupération et calcul des agrégats en cours..."):
        data = fetch_data(st.session_state['api_date'], st.session_state['api_airline'])

    if data:
        st.markdown("<br>", unsafe_allow_html=True)
        
        # --- SECTION 1 : KPIs ---
        st.markdown(
            """
            <h3 style='display: flex; align-items: center;'>
                <img src='https://img.icons8.com/color/48/000000/combo-chart--v1.png' width='35' style='margin-right: 10px;'>
                Indicateurs de Performance
            </h3>
            """, unsafe_allow_html=True
        )
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        total_vols = data.get("total_flights", 0)
        arrived = data.get("statuses", {}).get("ARRIVED", 0)
        scheduled = data.get("statuses", {}).get("SCHEDULED", 0)
        completion_rate = (arrived / total_vols * 100) if total_vols > 0 else 0

        kpi1.metric("Vols Traités", f"{total_vols:,}")
        kpi2.metric("Vols Arrivés", f"{arrived:,}")
        kpi3.metric("Vols Programmés", f"{scheduled:,}")
        kpi4.metric("Taux de complétion", f"{completion_rate:.1f}%")

        st.markdown("---")

        # --- SECTION 2 : ANALYSES ---
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown(
                """
                <h3 style='display: flex; align-items: center;'>
                    <img src='https://img.icons8.com/color/48/000000/pie-chart--v1.png' width='30' style='margin-right: 10px;'>
                    Répartition par Statut
                </h3>
                """, unsafe_allow_html=True
            )
            status_df = pd.DataFrame(list(data["statuses"].items()), columns=["Statut", "Nombre"])
            
            if not status_df.empty:
                fig_status = px.pie(
                    status_df, values="Nombre", names="Statut", hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_status.update_layout(margin=dict(t=0, b=0, l=0, r=0))
                st.plotly_chart(fig_status, use_container_width=True)
            else:
                st.info("Aucune donnée de statut pour ces filtres.")

        with col_right:
            st.markdown(
                """
                <h3 style='display: flex; align-items: center;'>
                    <img src='https://img.icons8.com/color/48/000000/airplane-take-off.png' width='30' style='margin-right: 10px;'>
                    Top 5 des Compagnies
                </h3>
                """, unsafe_allow_html=True
            )
            airlines_df = pd.DataFrame(data["top_airlines"])
            
            if not airlines_df.empty:
                airlines_df["Nom Complet"] = airlines_df["airline_code"].apply(
                    lambda x: AIRLINE_MAPPING.get(x, f"Autre ({x})")
                )
                fig_airlines = px.bar(
                    airlines_df, x="count", y="Nom Complet", orientation='h', text_auto=True,
                    labels={"count": "Nombre de vols", "Nom Complet": ""},
                    color="count", color_continuous_scale="Viridis"
                )
                fig_airlines.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_airlines, use_container_width=True)
            else:
                st.info("Aucune donnée de compagnie pour ces filtres.")

        st.markdown("---")

        # --- SECTION 3 : ROUTES ---
        st.markdown(
            """
            <h3 style='display: flex; align-items: center;'>
                <img src='https://img.icons8.com/color/48/000000/map-marker--v1.png' width='30' style='margin-right: 10px;'>
                Itinéraires les plus fréquentés
            </h3>
            """, unsafe_allow_html=True
        )
        routes_df = pd.DataFrame(data["top_routes"])
        if not routes_df.empty:
            fig_routes = px.bar(
                routes_df, x="route", y="count",
                labels={"count": "Nombre de vols", "route": "Itinéraire (Origine ➔ Destination)"},
                color="count", color_continuous_scale="Blues"
            )
            st.plotly_chart(fig_routes, use_container_width=True)
        else:
            st.info("Aucune donnée d'itinéraire pour ces filtres.")
else:
    st.info("👆 Sélectionnez vos filtres et cliquez sur le bouton pour générer le dashboard.")