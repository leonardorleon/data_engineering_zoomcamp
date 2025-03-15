# Homework

For this homework we will be using the Taxi data:
- Green 2019-10 data from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz)


## Start Red Panda, Flink Job Manager, Flink Task Manager, and Postgres 

There's a `docker-compose.yml` file in the homework folder (taken from [here](https://github.com/redpanda-data-blog/2023-python-gsg/blob/main/docker-compose.yml))

Copy this file to your homework directory and run

```bash
docker-compose up
```

(Add `-d` if you want to run in detached mode)

Visit `localhost:8081` to see the Flink Job Manager

Connect to Postgres with [DBeaver](https://dbeaver.io/).

The connection credentials are:
- Username `postgres`
- Password `postgres`
- Database `postgres`
- Host `localhost`
- Port `5432`


In DBeaver, run this query to create the Postgres landing zone for the first events:
```sql 
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
)
```


## Question 1. Connecting to the Kafka server

We need to make sure we can connect to the server, so
later we can send some data to its topics

First, let's install the kafka connector (up to you if you
want to have a separate virtual environment for that)

```bash
pip install kafka-python
```

You can start a jupyter notebook in your solution folder or
create a script

Let's try to connect to our server:

```python
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

---------------------
**Answer 1:**

**version of redpanda, as shown by the docker-compose file: v24.2.18**

**you can also attach to the running container: `docker-compose exec redpanda-1 /bin/sh` and then run `rpk version` or `redpanda --version`**

---------------------

## Question 3: Sending the Trip Data

* Read the green csv.gz file
* We will only need these columns:
  * `'lpep_pickup_datetime',`
  * `'lpep_dropoff_datetime',`
  * `'PULocationID',`
  * `'DOLocationID',`
  * `'passenger_count',`
  * `'trip_distance',`
  * `'tip_amount'`

* Create a topic `green-trips` and send the data there with `load_taxi_data.py`
* How much time in seconds did it take? (You can round it to a whole number)
* Make sure you don't include sleeps in your code


---------------------
**Answer 2:**

**Question 2 is to create a kafka topic, this is done by:**

**`producer.send('green-trips', value=row)` in [hw_load_taxi_data.py](hw_load_taxi_data.py)**


---------------------


---------------------
**Answer 3:**

**Connecting to the kafka server, this can be done with the following code: [kafka_confirm_connection.py](kafka_confirm_connection.py)**

**Setting up the producer and running: `producer.bootstrap_connected()` will return true if properly connected**

---------------------


---------------------
**Answer 4:**

**It took 94 seconds to send the rows as they are read [load_taxi_data.py](../producers/load_taxi_data.py), and 211 seconds if selecting columns as shown in [hw_load_taxi_data.py](hw_load_taxi_data.py)**

---------------------


## Question 4: Build a Sessionization Window

* Copy `aggregation_job.py` and rename it to `session_job.py`
* Have it read from `green-trips` fixing the schema
* Use a [session window](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/) with a gap of 5 minutes
* Use `lpep_dropoff_datetime` time as your watermark with a 5 second tolerance
* Which pickup and drop off locations have the longest unbroken streak of taxi trips?


---------------------
**Answer 4:**

**See [session_job.py](session_job.py) for the answer**

**We will need a table in postgres to receive this data**

```sql
CREATE TABLE IF NOT EXISTS public.processed_aggregated_taxi_events
(
    pulocationid integer,
    dolocationid integer,
    window_start timestamp without time zone,
    window_end timestamp without time zone,
    total_trips_in_session integer
);
```

**Now that the table is created, we modify the job to fit the schema, and to use a session window**

**Finally, we run with the command `docker-compose exec jobmanager ./bin/flink run -py /opt/src/homework/session_job.py --pyFiles /opt/src`**

**The main modification on the code is the sql executed**

```sql
t_env.execute_sql(
    f"""
        INSERT INTO {postgres_sink}
        SELECT
            PULocationID,
            DOLocationID,
            SESSION_START(event_watermark,INTERVAL '5' MINUTES)  AS window_start,
            SESSION_END(event_watermark,INTERVAL '5' MINUTES)    AS window_end, 
            count(*)    AS total_trips_in_session
        FROM {source_table}
        GROUP BY 
            PULocationID, 
            DOLocationID, 
            SESSION(event_watermark,INTERVAL '5' MINUTES)
        ;
    """
).wait()
```

**The way that it works is by performing the session in the group by section of the query. Also the window start and window end need to be performed with the session_sart and session_end functions**

**In the groupby the watermark is used as well as the interval defined. This will introduce a record per session and count all the trips in that time period**

**To find the actual longes strip, we can query the resulting table**

```sql
select *, window_end - window_start as taxi_trips_streak 
from processed_aggregated_taxi_events 
order by taxi_trips_streak desc
limit 100;
```

**with the result being:**

| pulocationid | dolocationid | window_start       | window_end         | total_trips_in_session | taxi_trips_streak |
|--------------|--------------|--------------------|--------------------|------------------------|-------------------|
| 74           | 75           | 2019-10-21 08:48:10| 2019-10-21 09:58:42| 31                     | 01:10:32          |
| 95           | 95           | 2019-10-16 18:18:42| 2019-10-16 19:26:16| 44                     | 01:07:34          |
| 75           | 74           | 2019-10-02 17:12:14| 2019-10-02 18:17:27| 32                     | 01:05:13          |
| 223          | 223          | 2019-10-16 20:35:32| 2019-10-16 21:32:57| 30                     | 00:57:25          |
| 75           | 74           | 2019-10-22 17:23:28| 2019-10-22 18:16:39| 26                     | 00:53:11          |


**So the longest unbroken streak (also noting it's the same pu and do location): 01:10:32**


NOTE: I also tried a different way of doint it, as shown in [session_job_alternate.py](session_job_alternate.py) though I didn't complete it so it would work. The main difference is setting up the session in the FROM statement, which is different and it returns a "table-valued result" which needs extra steps to be properly aggregated before inserting into the sink. This can be looked into in the future. 

---------------------
