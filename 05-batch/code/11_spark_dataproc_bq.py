#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# Parse the rguments passed to the command line
parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

# Set the arguments to variables
input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# spark = SparkSession.builder \
#     .master("spark://sunlit-instance.europe-west1-d.c.sunlit-amulet-341719.internal:7077") \
#     .appName('test') \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

# Add temporary bucket to the spark configuration
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west1-975282657752-g9bacvgy')


# If we try to simply read the data without any extra config, we'll get a message saying there are no workers registered. We need to do that on the terminal
df_green = spark.read.parquet(input_green)

df_yellow = spark.read.parquet(input_yellow)


df_green = df_green \
            .withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
            .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')


df_yellow = df_yellow \
            .withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
            .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')


common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]


# Create a column to identify the type of taxi
df_green_sel = df_green \
                .select(common_columns) \
                .withColumn('service_type' , F.lit('green'))


# Create a column to identify the type of taxi
df_yellow_sel = df_yellow \
                .select(common_columns) \
                .withColumn('service_type' , F.lit('yellow'))


# make a union of both datasets
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupby('service_type').count().show()

df_trips_data.createOrReplaceTempView('trips_data')


df_result = spark.sql("""
SELECT
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

FROM 
    trips_data
GROUP BY 
    1,2,3
""")


df_result.count()

df_result.write.format('bigquery') \
    .option('table', output) \
    .save()