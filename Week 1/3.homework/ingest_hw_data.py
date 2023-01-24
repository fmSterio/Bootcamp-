import pandas as pd
from sqlalchemy import create_engine
import argparse
import requests
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name1 = params.table_name1
    table_name2 = params.table_name2
    url1 = params.url1
    url2 = params.url2

    response1 = requests.get(url1)
    response2 = requests.get(url2)

    with open('output1.csv.gz', 'wb') as f:
        f.write(response1.content)
    with open('output2.csv', 'wb') as f:
        f.write(response2.content)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter1 = pd.read_csv('output1.csv.gz', iterator=True,chunksize=100000)
    df_iter2 = pd.read_csv('output2.csv', iterator=True,chunksize=100000)

    df1 = next(df_iter1)
    df2 = next(df_iter2)

    df1.lpep_pickup_datetime = pd.to_datetime(df1.lpep_pickup_datetime)
    df1.lpep_dropoff_datetime = pd.to_datetime(df1.lpep_dropoff_datetime)
    

    # takes the header columns n-0 and to_sql created a table with the name yellow_taxi_data with given columns
    df1.head(n=0).to_sql(name=table_name1,con=engine,if_exists='replace')
    df2.head(n=0).to_sql(name=table_name2,con=engine,if_exists='replace')

    # append first chunk of data to the table
    df1.to_sql(name=table_name1,con=engine,if_exists='append')
    df2.to_sql(name=table_name2,con=engine,if_exists='append')

    while True:
        try:
            t_start = time()
            df1 = next(df_iter1)
            df1.lpep_pickup_datetime = pd.to_datetime(df1.lpep_pickup_datetime)
            df1.lpep_dropoff_datetime = pd.to_datetime(df1.lpep_dropoff_datetime)
            df1.to_sql(name=table_name1,con=engine,if_exists='append')
            t_end = time()
            print(f"Time to append 100000 rows form URL 1: {t_end-t_start}")
        except StopIteration:
            print("Iteration is stopped.")
            break
    while True:
        try:
            t_start = time()
            df2 = next(df_iter2)
            df2.to_sql(name=table_name2,con=engine,if_exists='append')
            t_end = time()
            print(f"Time to append 100000 rows form URL 2: {t_end-t_start}")
        except StopIteration:
            print("Iteration is stopped.")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres' )
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', type=str, help='database name for postgres')
    parser.add_argument('--table_name1', help='table name for postgres')
    parser.add_argument('--table_name2', help='table name for postgres')
    parser.add_argument('--url1', help='first url for csv file')
    parser.add_argument('--url2', help='second url for csv file')
    args = parser.parse_args()
    main(args)







