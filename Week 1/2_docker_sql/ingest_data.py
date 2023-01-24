#!/usr/bin/env python
# coding: utf-8



import pandas as pd
from sqlalchemy import create_engine
from time import time 
import argparse
import requests


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name ='output.csv'


    #download the csv file from the url
    #os.system(f"wget {url} -O {csv_name}")
   
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    
    #download the csv file from the url
    response = requests.get(url)
    with open(csv_name, 'wb') as f:
        f.write(response.content)
        
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True,chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # takes the header columns n-0 and to_sql created a table with the name yellow_taxi_data with given columns 
    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')

    # append first chunk of data to the table 
    df.to_sql(name=table_name,con=engine,if_exists='append')


    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name,con=engine,if_exists='append')
            t_end = time()
            print('inserted another chunk, took %.3f' % (t_end - t_start))
        except StopIteration:
            print('finished ingesting data')
            break
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into POstgres')

    parser.add_argument('--user', help='user name for postgres ')
    parser.add_argument('--password', help='password for postgres ')
    parser.add_argument('--host', help='host for postgres ')
    parser.add_argument('--port', help='port for postgres ')
    parser.add_argument('--db', help='database name for postgres ')
    parser.add_argument('--table_name', help='table name for postgres ')
    parser.add_argument('--url', help='url for csv file ')

    args = parser.parse_args()
    main(args)








