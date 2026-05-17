"""
Vérification rapide de l'ingestion AF/KLM (API + Supabase).
Exécuter : python 1_ingestion/verify_ingestion.py
"""
import os
import sys
from pathlib import Path

# Charger .env si présent (comme dlt)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

# 1. Vérifier les variables d'environnement
print("=== 1. Variables d'environnement ===")

api_key = os.getenv("AF_CLIENT_ID_1")
db_host = os.getenv("DB_HOST")
db_pass = os.getenv("DB_PASSWORD")

print(f"  AF_CLIENT_ID_1: {'✓ defini' if api_key else 'X manquant'}")
print(f"  DB_HOST:        {'✓ defini' if db_host else 'X manquant'}")
print(f"  DB_PASSWORD:    {'✓ defini' if db_pass else 'X manquant'}")

if not api_key:
    print("\n-> Erreur : AF_CLIENT_ID_1 est absent de ton fichier .env")
    sys.exit(1)

# 2. Test API (1 requête)
print("\n=== 2. Test API AF/KLM ===")
import requests

url = "https://api.airfranceklm.com/opendata/flightstatus"
params = {
    "startRange": "2026-01-16T00:00:00.000Z", # Date de test fixée en 2026
    "endRange": "2026-01-16T01:00:00.000Z",
    "pageSize": 5,
    "pageNumber": 0,
}
headers = {"API-Key": api_key, "Accept": "application/hal+json"}
try:
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    flights = data.get("operationalFlights", [])
    print(f"  ✓ API OK — {len(flights)} vols recuperes (page 0)")
except requests.exceptions.RequestException as e:
    print(f"  X Erreur API: {e}")
    sys.exit(1)

# 3. Test connexion Supabase (si credentials présents)
if db_host and db_pass:
    print("\n=== 3. Test connexion Supabase ===")
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=db_host,
            port=os.getenv("DB_PORT", "5432"), 
            database=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER"),
            password=db_pass,
            sslmode="require",
            connect_timeout=10,
        )
        cur = conn.cursor()
        
        # AJOUT : Utilisation du schéma dynamique 'bronze' spécifié dans ton .env
        schema = os.getenv("DB_SCHEMA", "public")
        cur.execute(f"SELECT COUNT(*) FROM {schema}.operational_flights")
        
        n = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✓ Connexion OK — {n} lignes dans {schema}.operational_flights")
    except Exception as e:
        print(f"  X Erreur DB: {e}")
        sys.exit(1) # AJOUT : Force le statut en échec Airflow si la base ne répond pas
else:
    print("\n=== 3. Connexion Supabase ===")
    print("  (ignore — DB_HOST ou DB_PASSWORD manquant)")

print("\n=== Verification terminee ===")