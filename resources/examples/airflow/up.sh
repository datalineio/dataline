#!/usr/bin/env bash
cd ../../..
docker-compose down -v
docker-compose up -d
cd resources/examples/airflow || exit
docker-compose down -v
docker-compose up -d
echo "Access Airbyte at http://localhost:8000 and set up a connection."
echo "Enter your Airbyte connection ID: "
read connection_id
docker exec -ti airflow_webserver airflow variables set 'AIRBYTE_CONNECTION_ID' "$connection_id"
docker exec -ti airflow_webserver airflow connections add 'airbyte_example' --conn-uri 'airbyte://host.docker.internal:8000'
echo "Access Airflow at http://localhost:8085 to kick off your Airbyte sync DAG."