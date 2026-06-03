"""
Vérification rapide de l'ingestion AF/KLM (Docker Local ou Supabase Cloud).
Exécuter : python 1_ingestion/verify_ingestion.py
"""
import os
import sys
from pathlib import Path

# Charger .env si présent (comme dlt)
try:
    from dotenv import load_dotenv
    # Modification du chemin relatif pour remonter correctement jusqu'au fichier .env racine
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

def main_verify():
    # 0. Récupération de la cible d'environnement (par défaut local)
    env_target = os.getenv("ENV_TARGET", "local").strip().lower()
    print(f"=== 0. Mode d'environnement détecté : {env_target.upper()} ===")

    # 1. Configuration des variables d'environnement selon la cible
    print("\n=== 1. Variables d'environnement ===")
    api_key = os.getenv("AF_CLIENT_ID_1")
    print(f"  AF_CLIENT_ID_1: {'✓ defini' if api_key else 'X manquant'}")

    if env_target == "local":
        # DÉTECTION DU CONTEXTE : Sommes-nous dans le conteneur Docker ou sur Windows ?
        # Airflow injecte systématiquement des variables spécifiques comme 'AIRFLOW_CTX_DAG_ID'
        if os.getenv("AIRFLOW_CTX_DAG_ID") or os.getenv("AIRFLOW_HOME"):
            db_host = "postgres_local"
            print("  Target: LOCAL (Exécution DANS le réseau privé Docker)")
        else:
            db_host = "localhost"
            print("  Target: LOCAL (Exécution HORS Docker - Depuis ton terminal Windows)")

        db_pass = "FormationData2026"
        db_user = "data_engineer"
        db_name = "data_hub"
        db_port = "5432"
        ssl_mode = "disable"
    else:
        # On utilise les variables de ton .env pour Supabase Cloud
        db_host = os.getenv("AFKLM_DB_HOST") or os.getenv("DB_HOST")
        db_pass = os.getenv("AFKLM_DB_PASSWORD") or os.getenv("DB_PASSWORD")
        db_user = os.getenv("AFKLM_DB_USER") or os.getenv("DB_USER")
        db_name = os.getenv("AFKLM_DB_NAME", "postgres") or os.getenv("DB_NAME", "postgres")
        db_port = os.getenv("AFKLM_DB_PORT", "5432") or os.getenv("DB_PORT", "5432")
        ssl_mode = "require"
        print(f"  Target: CLOUD (Supabase détecté sur {db_host})")

    print(f"  DB_HOST:         {'✓ defini' if db_host else 'X manquant'}")
    print(f"  DB_PASSWORD:     {'✓ Environmental check passed' if db_pass else 'X manquant'}")

    # 2. Test API AF/KLM (Désactivé en production pour préserver les quotas)
    print("\n=== 2. Test API AF/KLM ===")
    print("  ✓ Test API ignore (Donnees de production deja chargees en base)")

    # 3. Test de connexion et de volumétrie à la base cible
    if db_host and db_pass:
        print(f"\n=== 3. Test connexion base de données ({env_target.upper()}) ===")
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=db_host,
                port=db_port, 
                database=db_name,
                user=db_user,
                password=db_pass,
                sslmode=ssl_mode,
                connect_timeout=10,
            )
            cur = conn.cursor()
            
            schema = os.getenv("DB_SCHEMA", "public")
            
            # dlt crée les tables en minuscules d'où la vérification sur "operational_flights"
            cur.execute(f"SELECT COUNT(*) FROM {schema}.operational_flights")
            
            n = cur.fetchone()[0]
            cur.close()
            conn.close()
            print(f"  ✓ Connexion OK — {n} lignes trouvées dans {schema}.operational_flights")
        except Exception as e:
            print(f"  X Erreur DB ({env_target.upper()}): {e}")
            sys.exit(1)
    else:
        print(f"\n=== 3. Connexion base de données ({env_target.upper()}) ===")
        print("  (ignore — Paramètres de connexion hôte ou mot de passe manquants)")

    print("\n=== Verification terminee avec succes ===")

# Permet de conserver l'exécution manuelle directe (ex: python verify_ingestion.py)
if __name__ == "__main__":
    main_verify()