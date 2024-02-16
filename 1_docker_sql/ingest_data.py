import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = 'output.csv'
    url = params.url

    #download the CSV
    os.system(f"wget {url} -O {csv_name}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df=next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime) #text转timestamp
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name = table_name,con=engine,if_exists='replace')
    %time df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start=time()
        df = next(df_iter)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime) #text转timestamp
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('insert another chunk... took %.3f sec' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        description='Ingest CSV to Postgres')
    # user,password, host, port, database name, table name, url of csv
    parser.add_argument('user', help='user name for postgres')
    parser.add_argument('password', help='pw for postgres')
    parser.add_argument('host', help='hostfor postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table-name', help='name of table where we write results to')
    parser.add_argument('url', help='url of csv file')

    args = parser.parse_args()
