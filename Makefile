SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

docker-image:
	# docker build -f ./src/server/Dockerfile -t "server:latest" .
	# docker build -f ./src/client/Dockerfile -t "client:latest" .
	docker build -f ./src/watchdog/Dockerfile -t "watchdog:latest" .
	# docker build -f ./src/propagator/Dockerfile -t "propagator:latest" .
	# docker build -f ./src/nodes/trimmer/Dockerfile -t "trimmer:latest" .
	# docker build -f ./src/nodes/filters/genre/Dockerfile -t "genre:latest" .
	# docker build -f ./src/nodes/filters/score/Dockerfile -t "score:latest" .
	# docker build -f ./src/nodes/filters/release_date/Dockerfile -t "release_date:latest" .
	# docker build -f ./src/nodes/filters/english/Dockerfile -t "english:latest" .
	# docker build -f ./src/nodes/counters/os_counter/Dockerfile -t "os_counter:latest" .
	# docker build -f ./src/nodes/counters/avg_counter/Dockerfile -t "avg_counter:latest" .
	# docker build -f ./src/nodes/joiners/q3_joiner/Dockerfile -t "q3_joiner:latest" .
	# docker build -f ./src/nodes/joiners/q4_joiner/Dockerfile -t "q4_joiner:latest" .
	# docker build -f ./src/nodes/joiners/q5_joiner/Dockerfile -t "q5_joiner:latest" .
	# docker build -f ./src/replicas/os_counter_replica/Dockerfile -t "os_counter_replica:latest" .
	# docker build -f ./src/replicas/avg_counter_replica/Dockerfile -t "avg_counter_replica:latest" .
	# docker build -f ./src/replicas/q3_joiner_replica/Dockerfile -t "q3_joiner_replica:latest" .
	# docker build -f ./src/replicas/q4_joiner_replica/Dockerfile -t "q4_joiner_replica:latest" .
	# docker build -f ./src/replicas/q5_joiner_replica/Dockerfile -t "q5_joiner_replica:latest" .
	# docker build -f ./src/replicas/propagator_replica/Dockerfile -t "propagator_replica:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

generate-compose:
	./scripts/generar-compose.sh
.PHONY: generate-compose

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 10
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

docker-compose-up-logs: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs