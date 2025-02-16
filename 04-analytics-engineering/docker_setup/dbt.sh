#!/bin/bash
docker-compose -f /home/leo/leo_data_engineering/04-analytics-engineering/docker_setup/docker-compose.yaml run --workdir="//usr/app/dbt/taxi_rides_ny" dbt-bq-dtc "$@"
