#!/bin/bash

echo "Running Manager Docker container..."

docker-compose build

docker-compose up -d rabbitmq

docker-compose run --rm --name worker-manager manager