.PHONY: up down build restart logs ps clean

up:
	docker compose up -d

build:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f

logs-producer:
	docker compose logs -f producer

logs-consumer:
	docker compose logs -f spark-consumer

logs-neo4j:
	docker compose logs -f neo4j

ps:
	docker compose ps

queries:
	docker compose run --rm producer python -m src.queries

clean:
	docker compose down -v --remove-orphans