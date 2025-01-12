# Data orchestration

## Basic concepts

### What is a Data Lake?

A Data Lake consists of a central repository where any type of data, either structured or unstructured, can be stored. The main idea behind a Data Lake is to ingest and make data available as quickly as possible inside an organization.

### Data Lake vs. Data Warehouse

A Data Lake stores a huge amount of data and are normally used for stream processing, machine learning and real time analytics. On the other hand, a Data Warehouse stores structured data for analytics and batch processing.

### Extract-Transform-Load (ETL) vs. Extract-Load-Transform (ELT)

ETL is usually a Data Warehouse solution, used mainly for small amount of data as a schema-on-write approach. On the other hand, ELT is a Data Lake solution, employed for large amounts of data as a schema-on-read approach.

## Workflow orchestration

It means governing your dataflow in a way that respects orchestration rules and your business logic.

A dataflow defines all extraction and processing steps that the data will be submitted to, also detailing any transformation and intermediate states of the dataset. For example, in an ETL process, a dataset is first extracted (E) from some source (e.g., website, API, etc), then transformed (T) (e.g., dealing with corrupted or missing values, joining datasets, datatype conversion, etc) and finally loaded (L) to some type of storage (e.g., data warehouse). For more details, read [What is Data Flow?](https://www.modernanalyst.com/Careers/InterviewQuestions/tabid/128/ID/6119/What-is-a-Data-Flow.aspx) and [Extract, transform, load](https://en.wikipedia.org/wiki/Extract,_transform,_load).

* Dataflows can be scheduled, run and observed.
* A good orchestration service should scale and be highly available.

A workflow orchestration tool allows us to manage and visualize dataflows, while ensuring that they will be run according to a set of predefined rules. A good workflow orchestration tool makes it easy to schedule or execute dataflows remotely, handle faults, integrate with external services, increase reliability, etc. For more information, read [Workflow Orchestration vs. Data Orchestration — Are Those Different?](https://towardsdatascience.com/workflow-orchestration-vs-data-orchestration-are-those-different-a661c46d2e88) and [Your Code Will Fail (but that’s ok)](https://medium.com/the-prefect-blog/your-code-will-fail-but-thats-ok-f0327a208dbe).

Normally, when talking about data workflows, we talk about DAGs (Directed Acyclic Graph). Some data workflow orchestration tools are:

- Apache Airflow
- Luigi
- Prefect
- Mage

### Note: Re-run the database and pgadmin containers

They are located on week 1 dockerfiles directory

```
docker-compose -f C:\Users\Usuario\Documents\00_PROGRAMMING\data_engineering_zoomcamp\01-docker-terraform\02_docker_sql\dockerfiles up -d
```

## Airflow Setup

![Airflow-architecture](images\airflow-architecture.jpeg "Airflow Arichitecture")

### Web Server

Graphical user interface to check, trigger and debug the behavior of dags. This is available in localhost 8080

### Scheduler

Component responsible for scheduling jobs, it handles triggering and scheduling workflows, submits tasks to the executor to run, monitor tasks and dags and triggers the task instances once their dependencies are complete.

### Worker

Is a component that executes a task given by the scheduler.

## Metadata Database

Is the backend to the airflow environment, it is used by the executor and web server to store the state of the environment. 

There are other optional components like the data server, redis server -message broker from scheduler to worker-, a flower app, for launching the environment -localhost 5555-, an airflow init service for the initialization service custom to the setup (credentials, env variables, etc.).