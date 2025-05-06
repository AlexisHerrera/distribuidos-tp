#!/bin/bash

# Compare Q result files from all users in .results

for i in {1..5}; do
    find .results/ -name \*q${i}\* | xargs diff
done
