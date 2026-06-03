.PHONY: init start stop restart logs-web logs-scheduler status shell-scheduler shell-webserver down

# First-time setup: initialise the DB and create the admin user
init:
	docker-compose up airflow-init

# Start webserver + scheduler (run `make init` first if fresh install)
start:
	docker-compose up -d airflow-webserver airflow-scheduler

# Stop everything without wiping data
stop:
	docker-compose stop

# Stop, then start again (keeps the database)
restart:
	docker-compose stop airflow-webserver airflow-scheduler
	docker-compose up -d airflow-webserver airflow-scheduler

# Wipe everything including the database — next step must be `make init`
down:
	docker-compose down -v

# Full reset: wipe and reinitialise in one command
reset: down init

# Logs
logs-web:
	docker-compose logs -f airflow-webserver

logs-scheduler:
	docker-compose logs -f airflow-scheduler

# Container status
status:
	docker-compose ps

# Shell access inside containers
shell-scheduler:
	docker-compose exec airflow-scheduler bash

shell-webserver:
	docker-compose exec airflow-webserver bash
