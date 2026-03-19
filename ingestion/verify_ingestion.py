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
api_key = os.getenv("AFKLM_API_KEY")
db_host = os.getenv("AFKLM_DB_HOST")
db_pass = os.getenv("AFKLM_DB_PASSWORD")
print(f"  AFKLM_API_KEY: {'✓ défini' if api_key else '✗ manquant'}")
print(f"  AFKLM_DB_HOST: {'✓ défini' if db_host else '✗ manquant'}")
print(f"  AFKLM_DB_PASSWORD: {'✓ défini' if db_pass else '✗ manquant'}")

if not api_key:
    print("\n→ Définir AFKLM_API_KEY :")
    print("    export AFKLM_API_KEY='votre_clé'")
    print("  ou créer un fichier .env à la racine avec AFKLM_API_KEY=...")
    sys.exit(1)

# 2. Test API (1 requête)
print("\n=== 2. Test API AF/KLM ===")
import requests

url = "https://api.airfranceklm.com/opendata/flightstatus"
params = {
    "startRange": "2025-03-10T00:00:00.000Z",
    "endRange": "2025-03-10T23:59:59.000Z",
    "pageSize": 10,
    "pageNumber": 0,
}
headers = {"API-Key": api_key, "Accept": "application/hal+json"}
try:
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    flights = data.get("operationalFlights", [])
    print(f"  ✓ API OK — {len(flights)} vols récupérés (page 0)")
except requests.exceptions.RequestException as e:
    print(f"  ✗ Erreur API: {e}")
    sys.exit(1)

# 3. Test connexion Supabase (si credentials présents)
if db_host and db_pass:
    print("\n=== 3. Test connexion Supabase ===")
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=db_host,
            port=os.getenv("AFKLM_DB_PORT", "5432"),
            database=os.getenv("AFKLM_DB_NAME", "postgres"),
            user=os.getenv("AFKLM_DB_USER", "postgres"),
            password=db_pass,
            sslmode=os.getenv("AFKLM_DB_SSLMODE", "require"),
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM public.operational_flights")
        n = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✓ Connexion OK — {n} lignes dans operational_flights")
    except Exception as e:
        print(f"  ✗ Erreur DB: {e}")
else:
    print("\n=== 3. Connexion Supabase ===")
    print("  (ignoré — AFKLM_DB_HOST ou AFKLM_DB_PASSWORD manquant)")

print("\n=== Vérification terminée ===")
