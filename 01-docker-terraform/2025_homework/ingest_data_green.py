
# coding: utf-8

import argparse
# import subprocess
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.gz"  # File is now stored in a gzip 

    # download csv
    # subprocess.call(["wget", url, "-O", csv_name])
    #download parquet
    os.system(f'wget {url} -O {csv_name}')

    df = pd.read_csv(csv_name, compression="gzip", nrows=5)

    # convert date columns into datetime
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Let's make a connection to postgres with pandas
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # In order to ingest it to postgres we need to generate a schema
    print("Generated schema:")
    print(pd.io.sql.get_schema(df,name=table_name,con=engine))

    # First create table by running command with a head(n=0), as it will only create the schema
    df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')

    # Let's now grab the dataframe with an iterator to batch ingest it into postgres
    df_iter = pd.read_csv(csv_name, compression="gzip", iterator=True, chunksize=100000)

    # Now we do the same as with the header, but actually append the chunks of the dataframe iterator

    for chunk in df_iter:
        t_start = time()
        # get next chunk of dataframe 
        df = chunk 

        # convert date columns into datetime
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
        df.to_sql(name=table_name,con=engine, if_exists='append')
        
        t_end = time()
        
        print(f"inserted another chunk..., it took {t_end - t_start :.3f}")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')
        
    # user, password, host, port, database name, table name, url of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where the results will be written to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)