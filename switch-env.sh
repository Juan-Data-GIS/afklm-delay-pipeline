#!/bin/bash
# Usage: source ./switch-env.sh dev | prod
# IMPORTANT : utiliser avec "source" pour que les exports soient actifs dans le shell courant
#   source ./switch-env.sh dev
#   source ./switch-env.sh prod

TARGET="${1:-}"

if [[ "$TARGET" != "dev" && "$TARGET" != "prod" ]]; then
    echo "Usage: source ./switch-env.sh dev | prod"
    echo ""
    echo "  dev  → afklm_delay_db_dev  (AWS eu-west-1)"
    echo "  prod → afklm_delay_db_prod (AWS eu-central-1)"
    return 1 2>/dev/null || exit 1
fi

ENV_FILE=".env.${TARGET}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Erreur : fichier $ENV_FILE introuvable."
    return 1 2>/dev/null || exit 1
fi

cp "$ENV_FILE" .env
set -a
source .env
set +a

HOST=$(grep AFKLM_DB_HOST .env | cut -d= -f2)
echo "Environnement actif : ${TARGET} (${HOST})"
