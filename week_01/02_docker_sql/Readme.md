# Review of Docker and SQL <!-- omit in toc -->

This section is a simple introduction/review of Docker and SQL. The goal is to start a postgres instance in a docker container and ingest data from the popular ny yellow taxi dataset. 

- [Setting up postgres and pgAdmin](#setting-up-postgres-and-pgadmin)
  - [Creating a postgres container manually](#creating-a-postgres-container-manually)
  - [Creating an ingestion script on a jupyter notebook](#creating-an-ingestion-script-on-a-jupyter-notebook)
  - [Running pgAdmin and linking docker containers](#running-pgadmin-and-linking-docker-containers)
    - [Creating a docker network](#creating-a-docker-network)
- [Docker-compose and ingestion script](#docker-compose-and-ingestion-script)
  - [dockerizing the ingestion script](#dockerizing-the-ingestion-script)

# Setting up postgres and pgAdmin

## Creating a postgres container manually

Before diving into docker-compose, we can create simple docker containers through the cli. Below is an example for creating the postgres instance we will be using.

Notes:
- -e is used for environment variables, some of which are needed for the official postgres container
- -v allows to attach a volume from the host to the container. In this case, since we want persistance of our data, we connect the container data folder with our own on the host machine
- -p for specifying port. We are using the default one, so it's not strictly necessary

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

To connect to the database, you can use something like `pgcli`:

    pgcli -h localhost -p 5432 -u root -d ny_taxi

## Creating an ingestion script on a jupyter notebook

This [notebook](ingesting_ny_taxi_dataset.ipynb) shows a preliminary way of ingesting the data in chunks. This needs to be cleaned up and made production ready, but it's a simple starting point.

## Running pgAdmin and linking docker containers

Of cource, pgcli is not the most convinient way to run queries, so we might want to use something like pgAdmin to have a more user friendly interface. This can also be done through docker, but we need to make sure both our containers (postgres and pgAdmin) are in the same network so they can talk to each other.

To create a pgAdmin container through docker cli:

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

### Creating a docker network

Simply create a docker network and give it a name

    docker network create pg-network

Now simply add the --network and --name tags to both our postgres and pgAdmin containers.

database:

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name=pg-database \
  postgres:13
```

pgAdmin:

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name=pgadmin \
  dpage/pgadmin4
```

# Docker-compose and ingestion script

For the ingestion script, the code from the notebook was cleaned up to use paramters in [dockerfiles/ingest_data.py](ingest_data.py). Simply run by:

Note: for simplicity we are passing passwords directly, but it should be done by environment variables in a real use case.

```bash
URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

python3 ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

## dockerizing the ingestion script

The requirements for the ingestion script were put in the [Dockerfile](dockerfiles/Dockerfile) and then it's just a matter of building the container with the parameters needed for ingestion

```bash
docker build -t taxi_ingest:v001 .
```

```bash
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```