#!/bin/bash 

echo "Running Worker Docker container..."

docker-compose build

docker-compose run -d --rm worker