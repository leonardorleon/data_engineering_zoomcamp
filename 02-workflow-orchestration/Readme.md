# Workflow orchestration with Kestra

## 1. Introduction to Orchestration and Kestra

* What is orchestration?

There are multiple elements working together on the data pipelines. The orchestration part relates to the idea of making all these elements work together and in syncrhony to achieve complete DE jobs.

* What is Kestra?

Kestra is an all-in-one orchestration platform. It can be used in a schedule or even as an event driven system. 

There are multiple ways to work with Kestra, ranging from no code to full code. There are multiple languages that can be used with Kestra. 

Another importand addition is the monitoring tools included in kestra. 

## Sope of the Kestra module:

- Introduction to Kestra
- ETL to postgres
- ETL to Google Cloud
- Parametrizing Execution
- Scheduling and Backfills
- Install Kestra on the cloud and sync flows with Git


## Some Kestra concepts:

- Workflows are known as flows
- It is declared in YAML
- Works in any language

### Important properties

- ID: name of the flow
- Namespace: envionment of a flow
- Tasks: List of tasks to execute in a flow


### Inputs

Inputs in Kestra can be defined at the top and be reused like constant values in various programming languages.

### Outputs

The outputs are defined so you can use the outputs from a task in a different one later on.

### Triggers

Triggers are a way to execute flows based on conditions such as schedules, webhook or something else. 

## Fundamentals of Kestra:

There are three main properties:

* id
* namespace
* tasks ( they themselves have properties such as id and type)

We can additionally add a description and labels, which will serve for documentation and organizing via metadata.

## Using inputs

Inputs are set up at the top and are sort of like constants in programming languages. The most common ones are strings, integers and booleans.

https://kestra.io/docs/tutorial/inputs

## Outputs

Outputs are a good way to pass data between tasks. The outputs tab can be used to explore the outputs on a given task, but additionally they can be used by other tasks like a variable. 

to retrieve outputs `{{ outputs.task_id.output_property }}`

https://kestra.io/docs/tutorial/outputs

## Triggers

You can set up triggers to set up executions on a schedule, normally using a cron expression.

You can have multiple triggers, so it's not necessary to rely on a single source to start the task. 

https://kestra.io/docs/tutorial/triggers

## Controlling orchestration logic

A common orchestration requirement is to execute processes in parallel. For example, processing data for each partition in parallel, it can significantly speed up processing time. 

An example of this is the `EachParallel` flowable task.

https://kestra.io/docs/tutorial/flowable

## Handling Errors and Retries

Since errors are inevitable, we can use error handling to make it smoother and to monitor when things go wrong. 

The example below uses the error property which is similar to tasks but it's used to specifically handle error and in the example above to send a message about it.

There is also retry logic, which can allow to repeat executions in a controlled way and actually fail if the maximun number of retries is exceeded. 

https://kestra.io/docs/tutorial/errors

## Docker inside of Kestra

Many components of kestra are run in docker containers. For example python scripts can be set up with a specific image, be it a docker general image or a custom image. This allows to isolate executions and avoid conflicts between various tasks running in the same namespace. 

## ETL Pipelines with Postgres in Kestra

https://www.youtube.com/watch?v=OkfLX28Ecjg&list=PLEK3H8YwZn1oPPShk2p5k3E9vO-gPnUCf&index=3

The goal for this section is to create an ETL for both yellow taxi data and green taxi data. Backups for these can be found here:

https://github.com/DataTalksClub/nyc-tlc-data/releases

The first step is to set up the docker-compose. I've set it up just as shown in the documentation for kestra, but I've additionally added another postgres database just as we did in week 1 to work with the taxi data separately from anything related to kestra. Also, I've added the pgadmin to the docker compose in order to view and query the data on the database. 

### Set up kestra and postgres

See the [docker-compose.yml](docker-compose.yml) for the set up of the various components. I've put together the set up from week 1 with this week's set up. Some things to note: I've added a second postgres container, so that the postgres for kestra can be used exclusively for that, and there is a postgres specifically for the zoomcamp data. I've also set up pgadmin on the same docket-compose.

Note that both postgres containers are on the default port 5432, this is not causing a problem when you can use the docker service name to differenciate between them.

The main difference from the video code I found was on the connection to postgres from kestra. In the video it looks like this: 

```yml
pluginDefaults:
  - type: io.kestra.plugin.jdbc.postgresql
    values:
      url: jdbc:postgresql://host.docker.internal:5432/postgres-zoomcamp
      username: kestra
      password: k3str4
```

While in my setup I had to change the `host.docker.internal` for the docker service name

```yml
pluginDefaults:
  - type: io.kestra.plugin.jdbc.postgresql
    values:
      url: jdbc:postgresql://pgdatabase:5432/postgres-zoomcamp      # Note: use docker service name for the conection since they're in the same docker network.
      username: root
      password: root
```


## Kestra and DBT

When setting up kestra and DBT I ran into many problems, basically what is recommended in the video for the postgres host in the dbt set up (to use `host.docker.internal`) does not work on linux.

I've tried to solve it in many different ways, none of them were working:

* Create a docker network and add it to all containers
* Set the docker network to bridge
* Add extra host:

    ```
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ```
* Use the name of the container like in the postgres plugin previously used

These are some of the things I tried, among others that didn't work.

I finally managed to get it working in a more "hardcoded" way, which was to find the actual ip of the docker network:

```
docker network inspect zoomcamp-network
```

And on the results, take the "gateway" ip from the docker network:

```
[
    {
        "Name": "zoomcamp-network",
        "Id": "a367202383d05ebdc234e0ef23f1dea120defb7bc65a4220c90a2ccb1e9b0a96",
        "Created": "2025-01-25T19:19:52.714900356Z",
        "Scope": "local",
        "Driver": "bridge",
        "EnableIPv6": false,
        "IPAM": {
            "Driver": "default",
            "Options": null,
            "Config": [
                {
                    "Subnet": "172.20.0.0/16",
                    "Gateway": "172.20.0.1"     <----- Used this one
                }
```

I would like to solve the hostname issue, but so far haven't been able to resolve it. 

Maybe the IPAM section of the docker network should be set up in the docker-compose file to ensure it's static and then use that in the connection between dbt and postgres.

## Kestra and GCP

As shown in the flows 04 through 07, the set up for GCP and big query is actually simpler than for DBT. Basically take the service account key and add it to the key values in kestra, as well as other items such as the project, database, bucket, region.

These can be used for identification when using the GCP plugin or when connecting DBT with the database.

The main difference that's worth noting is that in Big Query, when DBT asks for the dataset, you should use the project id and when it asks for a schema, you should use what BQ calls the dataset.

Once these steps are completed, the flows work properly.