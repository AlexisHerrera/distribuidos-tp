default: docker-compose-up

docker-build:
	docker compose -f docker-compose.yaml build
.PHONY: docker-build

docker-compose-up:
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

watch:
	watch 'docker ps -a --filter "network=tp-escalabilidad_testing_net" --format "table {{.ID}}\t{{.State}}\t{{.Names}}\t{{.Status}}"'
.PHONY: watch

# make restart service=client-1
restart:
	@# Check if 'service' variable is empty using shell syntax inside the recipe
	@if [ -z "$(service)" ]; then \
		echo "Error: Usage: make $@ service=<service_name>"; \
		exit 1; \
	fi
	@echo "Restarting service: $(service)..."
	docker compose -f docker-compose.yaml restart $(service)
.PHONY: restart

# make logs service=client-1
logs:
	@# Check if 'service' variable is empty using shell syntax inside the recipe
	@if [ -z "$(service)" ]; then \
		echo "Error: Usage: make $@ service=<service_name>"; \
		exit 1; \
	fi
	@echo "Following logs for service: $(service)..."
	docker compose -f docker-compose.yaml logs -f $(service)
.PHONY: logs
