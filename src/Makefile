.DEFAULT_GOAL := default
SHELL := /bin/bash

.PHONY: default up down build logs

default: up

up:
	bash -c 'set -a; . airflow.env; . hadoop.env; DOCKER_BUILDKIT=1 docker compose up --remove-orphans --build -d'

down:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose down'

build:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose build'

logs:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose logs -f'

stats:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose stats'

ps:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose ps'

restart:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose restart'

clean:
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose down -v'
	bash -c 'set -a; . airflow.env; . hadoop.env; docker compose down -v'
	bash -c 'set -a; . airflow.env; . hadoop.env; docker image prune -f'
