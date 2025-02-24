# Batch processing

There are multiple ways to process data, for example in batch and in streaming. This week we'll work with batch processing.

When we talk about batch processing, we take a large set of data, process it in some job and then can introduce it to a dataset. 

Batch processes are usually weekly, daily, hourly and even shorter periods in some cases.

## Technologies

Often we use python scripts for the ingestion and other processes, also sql for processing data as it is very popular. Other tools can be spark and flink for batch jobs, in this case we will focus on spark.

Normally we use some orchestrator, such as airflow to run the python scripts at a given schedule.

## Advantages of batch jobs

They are convenient and relatively easy to manage, they allow for retries as they don't happen in real time. They are generally scalable, so for a larger batch we can scale up the processing power for these jobs.

A possible dissadvantage is velocity, since for example an hourly job, we need to wait for the hour to finish, in addition to waiting for the jobs that are scheduled to complete. So you might have to wait 1.5 hours before you can see the data, depending on how long it takes to process. This might lead some jobs to be more suitable for streaming, where you need the inmediacy, but most of the times this is not the case and it's okay to wait.

Majority of data products and jobs tend to be in batch, while streaming is more for specific use cases.


## Apache Spark

Spark is a data processing engine that provides an interface for large-scale data processing.

Normally the process is in some data lake for example, and processed in spark and then it is saved processed into a datawarehouse most likely.

Spark is written in scala originally, but there are wrappers for other programming languages, including python `pyspark` which is a very popular option. 

Spark is used for both batch and streaming, even though in this module we will only cover batch processing with spark.

## When to use Spark?

Normally it is used when the data is in a datalake (S3/GCS), then process it in spark and then save it to a datalake or even a datawarehouse.

There are some tools to query directly s3 or datalakes with sql, such as hive, presto/athena which can be useful sometimes. If you need more flexibility than sql can provide, then you might want to use spark.

You can always mix and match, as in this typical batch job

![typical batch job](images/01_typical_batch_job.png)

