
# coding: utf-8

import os
import pandas as pd
import math
import numpy as np
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    csv_name = "output.gz"  # File is now stored in a gzip 

    # download csv
    #download parquet
    print("Download data")
    os.system(f'wget {url} -O {csv_name}')
    print("Read CSV to dataframe")
    df = pd.read_csv(csv_name, compression="gzip")

    # convert date columns into datetime
    print("Convert tpep_pickup_datetime and tpep_dropoff_datetime to datetime")
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passanger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0] 
    print(f"pre: missing passanger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, df, chunk_max_size):
    # Let's make a connection to postgres 
    connection_block = SqlAlchemyConnector.load("postgresconnection")
    
    with connection_block.get_connection(begin=False) as engine:
        # In order to ingest it to postgres we need to generate a schema
        print("Generated schema:")
        print(pd.io.sql.get_schema(df,name=table_name,con=engine))

        # First create table by running command with a head(n=0), as it will only create the schema
        df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')

        # Let's now split the dataframe in chunks to batch ingest it into postgres
        chunks = int(math.ceil(len(df) / chunk_max_size))
        rows_loaded = 0
        for df_chunk in np.array_split(df, chunks):
            t_start = time()
            # get next chunk of dataframe 
            df = df_chunk 
            
            df.to_sql(name=table_name,con=engine, if_exists='append')
            
            t_end = time()
            
            print(f"inserted another chunk..., it took {t_end - t_start :.3f}")
            rows_loaded += len(df)

        print(f"rows_loaded: {rows_loaded}")


@flow(name="subflow", log_prints=True)
def log_subflow(table_name : str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    # user, password, host, port, database name, table name, url of the csv

    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    chunk_max_size = 100000
    
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data, chunk_max_size)


if __name__=="__main__":
    main_flow("yellow_taxi_trips")
        
    