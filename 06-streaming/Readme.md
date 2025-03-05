# Streaming with Kafka

## What is kafka streaming?

The example for this class consists of a notice board. In the notice board there are topics, producers and consumers.

Since it's an essential element of kadka, what is a topic?

### Topic

Let's say we have an application which records the temperature every few seconds. So every 30 seconds it will record the temperature and send it out. Each of these events are a single data point. In kafka, we usuaally talk about logs, this is how data is shared inside of a topic. 

### Event

Each event contains a message, which can have certain information. In the example above, the message would be the timestamp and the temperature. A message usually has a timestamp, a key and a value.

## Why is kafka special?

It provides a lot of **robustness** and reliability to the topic, so even if the server is going down, you will still receive the data. This is due to replication, as it replicates the data over various nodes. Topics can be small, big, you can have many consumers. It provides a lot of **flexibility** on what we want to do. 

Kafka also provides **scalability**, it can handle increases of data size from 10 events per second to 1000 events per second and continue to work without issues.

## Stream processing and need for it

We used to work in monolithic architectures, which talked to a central database. They used to be genearlly big code repos which talked to multiple databases, but there were many issues with this type of application. Nowadays, the trend is to work in microservices which are smaller set of services which work together to complete the same functions than a monolithic architecture might have performed in the past.

The microservices can talk between them and they could be talking to central databases. This is generally okay when data is not too great, however when there are more microservices and increasing data, you need a streaming system to communicate between them. What usually happens is that we have a kafka topic, generally in terms of events. So whenever a microserivice wants to read from this kafka topic, they can do that. So each microservice might be reading from specific topics. 

Another thing that kafka can do for us is CDC (Change Data Capture), so for example a DB can write to the kafka topic and from there the different microservices can get that data. 