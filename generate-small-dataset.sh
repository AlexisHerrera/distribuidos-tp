#!/bin/bash

MOVIES_FILE=movies_metadata.csv
CREDITS_FILE=credits.csv
RATINGS_FILE=ratings.csv

create_small_file() {
    local filename=$1
    local total_lines=$(wc -l < ${filename})
    local small_file_lines=$((total_lines / 10))
    head -n ${small_file_lines} ${filename} > small-${filename}
}

create_small_file ${MOVIES_FILE}
create_small_file ${CREDITS_FILE}
create_small_file ${RATINGS_FILE}
