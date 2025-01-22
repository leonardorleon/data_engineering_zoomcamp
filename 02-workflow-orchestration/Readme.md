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

