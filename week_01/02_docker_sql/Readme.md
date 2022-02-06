# Review of Docker and SQL

This section is a simple introduction/review of Docker and SQL. The goal is to start a postgres instance in a docker container and ingest data from the popular ny yellow taxi dataset. 

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

This [notebook](ingesting_ny_taxi_dataset.ipynb) shows a preliminary way of ingesting the data in chunks. This needs to be cleaned up and made production ready.

## 