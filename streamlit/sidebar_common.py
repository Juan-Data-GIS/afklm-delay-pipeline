# streamlit/sidebar_common.py
"""Sidebar commune utilisée par toutes les pages Streamlit.

Streamlit multi-page génère automatiquement une nav sidebar en tête, qui
n'est pas suffisamment personnalisable (bloc unique impossible à scinder).
On la cache via CSS ([data-testid="stSidebarNav"]) et on recrée la nav
avec st.page_link() pour conserver l'indicateur de page active.
"""
import streamlit as st

URL_GITHUB_PROJET = "https://github.com/Juan-Data-GIS/afklm-delay-pipeline"
URL_FASTAPI_DOCS = "http://localhost:8000/docs"
URL_AIRFLOW = "http://localhost:8081"
URL_PROMETHEUS = "http://localhost:9090"
URL_GRAFANA = "http://localhost:3000"
URL_LIORA = "https://learn.datascientest.com/"
URL_SLIDES = "https://docs.google.com/presentation/d/1zrhrSkGNdkNAGvCf629LW4FBGnw0RLpC2AfKAt2dpMw/edit?usp=sharing"


def render_sidebar():
    with st.sidebar:
        # Bouton "retour Accueil" - st.page_link => target="_self" (même onglet)
        # + indicateur "page active" natif. Stylé via CSS ciblant stPageLink.
        st.page_link("Accueil.py", label="Accueil")

        st.divider()
        st.markdown("### Fonctionnalités")
        # st.page_link pour ouverture dans le même onglet (target="_self").
        # st.link_button forcerait target="_blank" (nouvel onglet), donc pas utilisé
        # ici pour la nav interne. Visuel "bouton secondaire" appliqué via CSS.
        st.page_link("pages/1_Observabilite.py", label="Observabilité")
        st.page_link("pages/2_Agregations.py", label="Agrégation")
        st.page_link("pages/3_Prediction_vol.py", label="Prédiction")

        st.divider()
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
        st.link_button("Slides de présentation", url=URL_SLIDES, type="primary", use_container_width=True)
