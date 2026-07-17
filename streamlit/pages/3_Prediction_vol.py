import streamlit as st
import requests
import json

# API Configuration
API_BASE_URL = "http://fastapi:8000"
st.set_page_config(page_title="Prédiction du retard par vol", layout="wide")

# Load CSS
def load_global_css():
    css_paths = ["dashboard_afklm.css", "../dashboard_afklm.css"]
    for path in css_paths:
        try:
            with open(path) as f:
                st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
                break
        except FileNotFoundError:
            continue

load_global_css()

# Cache API responses
@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_days():
    try:
        response = requests.get(f"{API_BASE_URL}/v1/analytics/days", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur lors de la récupération des jours: {e}")
        return []

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_flights_for_day(day):
    try:
        response = requests.get(f"{API_BASE_URL}/v1/analytics/day-query?day={day}", timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur lors de la récupération des vols pour le jour {day}: {e}")
        return None

# Process flight data
def process_flights(response):
    try:
        raw_data = response.json()
        flight_data = json.loads(raw_data)
        flight_data["combined_field"] = {
            key: f"{flight_data['flight_number'][key]} - {flight_data['airline_name'][key]} - "
                 f"{flight_data['departure_airport_name'][key]} --> {flight_data['arrival_airport_name'][key]}"
            for key in flight_data["flight_id"].keys()
        }
        return flight_data
    except Exception as e:
        st.error(f"Erreur lors du traitement des données de vol: {e}")
        return None

# Initialize session state
if "day_chosen" not in st.session_state:
    st.session_state.day_chosen = False

if "show_flights" not in st.session_state:
    st.session_state.show_flights = False

if "flight_data" not in st.session_state:
    st.session_state.flight_data = None

if "selected_flight" not in st.session_state:
    st.session_state.selected_flight = None

# Main app
st.markdown("### Prédiction de retard")

with st.container(border=True):
    st.markdown("**Prédiction de retard sur un vol**")

    # Fetch available days (cached)
    with st.spinner("Chargement des jours disponibles..."):
        raw_data = fetch_days()
        if not raw_data:
            st.error("Impossible de charger les jours disponibles.")
            st.stop()
        raw_data.sort()

    # Day selection
    option = st.selectbox(
        "Choisissez le jour de votre vol:",
        raw_data,
        key="day_selectbox"
    )

    # Reset flight data if a new day is selected
    if st.session_state.get("prev_day") != option:
        st.session_state.day_chosen = False
        st.session_state.show_flights = False
        st.session_state.flight_data = None
        st.session_state.selected_flight = None
        st.session_state.prev_day = option

    trigger_api_day = st.button("Afficher les vols", type="primary", on_click=lambda: st.session_state.update(day_chosen=True))

    if st.session_state.day_chosen:
        with st.spinner("Recherche des vols du jour..."):
            # Fetch flights for the selected day (cached)
            response = fetch_flights_for_day(option)
            if response:
                flight_data = process_flights(response)
                if flight_data:
                    st.session_state.flight_data = flight_data
                    flight_options = list(flight_data["combined_field"].values())
                    selected_flight = st.selectbox(
                        "Choisissez votre vol:",
                        flight_options,
                        key="flight_selectbox"
                    )

                    # Update selected flight in session state
                    st.session_state.selected_flight = selected_flight

                    trigger_api_flight = st.button(
                        "Sélectionner le vol",
                        type="primary",
                        on_click=lambda: st.session_state.update(show_flights=True),
                    )

                    # Only show delay result after clicking "Sélectionner le vol"
                    if st.session_state.show_flights and st.session_state.flight_data:
                        flight_id = None
                        for key, value in st.session_state.flight_data["combined_field"].items():
                            if value == st.session_state.selected_flight:
                                flight_id = st.session_state.flight_data["flight_id"][key]
                                break

                        if flight_id:
                            delay = st.session_state.flight_data["delay_predicted"][key]
                            if delay == 1:
                                st.info(f""":red[Le vol {flight_id} devrait avoir du retard.]""", icon="⏳")
                            elif delay == 0:
                                st.info(f""":green[Le vol {flight_id} ne devrait pas avoir de retard.]""", icon="👌")
                            else:
                                st.warning("Statut de retard inconnu.", icon="🤷")
                        else:
                            st.error("Vol non trouvé.")
                        st.session_state.show_flights = False