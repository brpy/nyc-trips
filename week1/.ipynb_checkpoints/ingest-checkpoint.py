import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    file = params.file

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(file, iterator=True, chunksize=100000)

    for n_chunk, df_chunk in enumerate(df_iter, start=1):
        t_start = time()
        
        df_chunk.tpep_pickup_datetime = pd.to_datetime(df_chunk.tpep_pickup_datetime)
        df_chunk.tpep_dropoff_datetime = pd.to_datetime(df_chunk.tpep_dropoff_datetime)
    
        # header
        if n_chunk == 1:
            df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
        # data rows
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print(f"Table: {table_name}, chunk : {n_chunk} inserted!, took {t_end - t_start:.3f}s seconds")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--file', required=True, help='csv file location')

    args = parser.parse_args()

    ingest(args)