#!/usr/bin/env bash
set -euo pipefail

SMALL_DATASET=${1:-no}
DATASET_PERCENT_SIZE=${2:-10}
SMALL_DATASET_FLAG=

if [[ ${SMALL_DATASET} != "no" ]]; then
  echo ">>> Generando small dataset"
  SMALL_DATASET_FLAG="-s"
  bash ./generate-small-dataset.sh ${DATASET_PERCENT_SIZE}
fi

echo ">>> Generando docker-compose-dev.yaml con ${SMALL_DATASET_FLAG}"
python generate-compose.py ${SMALL_DATASET_FLAG}

echo ">>> Lanzando contenedores"
# make docker-compose-up
