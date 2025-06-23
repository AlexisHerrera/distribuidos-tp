#!/bin/bash

grep -i 'container_name: ' docker-compose.yaml | cut -d':' -f 2 | awk '{$1=$1;print}' | grep -v 'rabbitmq\|client' > running_nodes


