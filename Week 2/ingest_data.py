#!/usr/bin/env python
# coding: utf-8



import pandas as pd
from sqlalchemy import create_engine
from time import time 
import argparse
import requests
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
 
@task(log_prints=True,retries=3)
def extract_data(url):

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    
    #download the csv file from the url
    response = requests.get(url)
    with open(csv_name, 'wb') as f:
        f.write(response.content)

    return csv_name


def transform_data(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(f"Pre:Transforming {len(df)} rows")
    df = df[df['passenger_count'] > 0]
    print(f"Post:Transforming {len(df)} rows")
    return df

@task(log_prints=True,retries=3)
def ingest_data(user,password,host,port,db,table_name,csv_name):


    df_iter = pd.read_csv(csv_name, iterator=True,chunksize=100000)

    df = next(df_iter)

    #create a database engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # takes the header columns n-0 and to_sql created a table with the name yellow_taxi_data with given columns 
    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
    # append first chunk of data to the table 
    df.to_sql(name=table_name,con=engine,if_exists='append')


    while True:
        try:
            t_start = time()
            df = transform_data(next(df_iter))
            df.to_sql(name=table_name,con=engine,if_exists='append')
            t_end = time()
            print('inserted another chunk, took %.3f' % (t_end - t_start))
        except StopIteration:
            print('finished ingesting data')
            break
    
@flow(name="Ingest flow")
def main_flow():
    user = 'root'
    passeord = 'root'
    host = 'localhost'
    port = '5432'
    db = 'ny_taxi'
    table_name = 'w2_yellow_taxi_data'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    
    csv_name = extract_data(url)
    print(csv_name)
    ingest_data(user,passeord,host,port,db,table_name,csv_name)


if __name__ == '__main__':
    main_flow()
    










