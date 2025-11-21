.DEFAULT_GOAL := default

.PHONY: default up down build logs

default: up

up:
	docker compose up --remove-orphans --build -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

stats:
	docker compose stats

ps:
	docker compose ps

restart:
	docker compose restart

clean:
	docker compose down -v
	docker compose down -v
	docker image prune -f
