#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run-compose.sh [use_small_dataset] [dataset_percentage] [num_clients]
# Where:
#   - use_small_dataset: 'yes' to use a small dataset, 'no' for the full dataset. Defaults to no.
#   - dataset_percentage: Percentage to shrink the dataset if use_small_dataset is 'yes'. Defaults to 10. Ignored if use_small_dataset is 'no'.
#   - num_clients: Number of client instances to create. Defaults to 1.
#
# Examples:
#
# Ex 1: Run with default settings (full dataset, 1 client)
# $ ./run-compose.sh
#
# Ex 2: Use small dataset (default 10%), 1 client
# $ ./run-compose.sh yes
#
# Ex 3: Use small dataset (40%), 1 client
# $ ./run-compose.sh yes 40
#
# Ex 4: Use full dataset, 3 clients
# $ ./run-compose.sh no 10 3
# In this execution, the second argument is ignored, but it should be passed.
#
# Ex 5: Use small dataset (30%), 5 clients
# $ ./run-compose.sh yes 30 5

SMALL_DATASET=${1:-no}
DATASET_PERCENT_SIZE=${2:-10}
NUM_CLIENTS=${3:-1}
GENERATE_COMPOSE_ARGS=""

if [[ "${SMALL_DATASET}" == "yes" ]]; then
  echo ">>> Generando small dataset con tamaño del ${DATASET_PERCENT_SIZE}%"
  GENERATE_COMPOSE_ARGS="${GENERATE_COMPOSE_ARGS} -s"
  bash ./generate-small-dataset.sh "${DATASET_PERCENT_SIZE}"
fi

echo ">>> Generando docker-compose.yaml con argumentos: '${GENERATE_COMPOSE_ARGS} -c ${NUM_CLIENTS}'"
GENERATE_COMPOSE_ARGS="${GENERATE_COMPOSE_ARGS} -c ${NUM_CLIENTS}"
python generate-compose.py ${GENERATE_COMPOSE_ARGS}

echo ">>> Lanzando contenedores"
make docker-compose-up