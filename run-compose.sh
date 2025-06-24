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
# Ex 1: Run with default settings (full dataset)
# $ ./run-compose.sh
#
# Ex 2: Use small dataset (default 10%)
# $ ./run-compose.sh yes
#
# Ex 3: Use small dataset (40%)
# $ ./run-compose.sh yes 40

SMALL_DATASET=${1:-no}
DATASET_PERCENT_SIZE=${2:-10}
GENERATE_COMPOSE_ARGS=""

if [[ "${SMALL_DATASET}" == "yes" ]]; then
  echo ">>> Generando small dataset con tamaño del ${DATASET_PERCENT_SIZE}%"
  GENERATE_COMPOSE_ARGS="${GENERATE_COMPOSE_ARGS} -s"
  bash ./generate-small-dataset.sh "${DATASET_PERCENT_SIZE}"
fi

echo ">>> Generando docker-compose.yaml con argumentos: '${GENERATE_COMPOSE_ARGS}'"
GENERATE_COMPOSE_ARGS="${GENERATE_COMPOSE_ARGS}"
python generate-compose.py ${GENERATE_COMPOSE_ARGS}

echo ">>> Generando running_nodes file"
bash ./generate-running-nodes.sh

echo ">>> Lanzando contenedores"
make docker-compose-up
