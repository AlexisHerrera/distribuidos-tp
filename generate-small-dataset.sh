#!/bin/bash

PERCENTAGE=${1:-10}

DIR_SMALL=".data-small"
DIR_WHOLE_DATA=".data"

mkdir -p ./${DIR_SMALL}

echo "Truncating files by ${PERCENTAGE}%"

MOVIES_FILE=movies_metadata.csv
CREDITS_FILE=credits.csv
RATINGS_FILE=ratings.csv

create_small_file() {
    local filename=$1
    local total_lines=$(wc -l < ${DIR_WHOLE_DATA}/${filename})
    local small_file_lines=$((total_lines / PERCENTAGE))
    head -n ${small_file_lines} ${DIR_WHOLE_DATA}/${filename} > ${DIR_SMALL}/${filename}
}

create_small_file ${MOVIES_FILE}
create_small_file ${CREDITS_FILE}
create_small_file ${RATINGS_FILE}
