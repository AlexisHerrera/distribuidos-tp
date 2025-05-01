#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run-compose.sh [yes/no] [number]
# Where number represents the percentage that the dataset will be shrinked
# in case the 'yes' is passed to use the small dataset
# Ex 1: run without flags is the same as running `python3 generate-compose.py`
# $ ./run-compose.sh
#
# Ex 2: run with 'yes' uses small dataset, by default is 10%
# $ ./run-compose.sh yes
# 
# Ex 3: run with 'yes' and specify percentage
# $ ./run-compose.sh yes 40

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
make docker-compose-up
