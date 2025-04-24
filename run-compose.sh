#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${1:-run_config.txt}"
FLAGS=()

while IFS='=' read -r key value; do
  [[ -z "$key" || "$key" =~ ^# ]] && continue
  if [[ "$value" =~ ^[0-9]+$ ]] && (( value > 0 )); then
    FLAGS+=( "--${key}" "${value}" )
  fi
done < "$CONFIG_FILE"

echo ">>> Generando docker-compose-dev.yaml con:" "${FLAGS[@]}"
python generate-compose.py "${FLAGS[@]}"

echo ">>> Lanzando contenedores"
make docker-compose-up
