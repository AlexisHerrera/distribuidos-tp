default: docker-compose-up

docker-image:
	docker build -f ./src/client/Dockerfile -t "client:latest" .
	docker build -f ./src/server/cleaner/Dockerfile -t "cleaner:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs