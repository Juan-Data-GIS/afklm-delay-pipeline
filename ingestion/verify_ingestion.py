"""
Vérification rapide de l'ingestion AF/KLM (Supabase uniquement).
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
print(f"  DB_HOST:         {'✓ defini' if db_host else 'X manquant'}")
print(f"  DB_PASSWORD:    {'✓ defini' if db_pass else 'X manquant'}")

# 2. Test API AF/KLM (Désactivé en production pour préserver les quotas)
print("\n=== 2. Test API AF/KLM ===")
print("  ✓ Test API ignore (Donnees de production deja chargees en base)")

# 3. Test connexion Supabase
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
        
        schema = os.getenv("DB_SCHEMA", "public")
        cur.execute(f"SELECT COUNT(*) FROM {schema}.operational_flights")
        
        n = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✓ Connexion OK — {n} lignes dans {schema}.operational_flights")
    except Exception as e:
        print(f"  X Erreur DB: {e}")
        sys.exit(1)
else:
    print("\n=== 3. Connexion Supabase ===")
    print("  (ignore — DB_HOST ou DB_PASSWORD manquant)")

print("\n=== Verification terminee ===")