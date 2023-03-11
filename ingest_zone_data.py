import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    username = params.username
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    url = params.url

    csv_name = 'output_zones.csv'

    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
    engine.connect()

    df = pd.read_csv(csv_name)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--username', help='Username for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--database', help='Database name for postgres')
    parser.add_argument('--table_name', help='Name of the table where we will write results')
    parser.add_argument('--url', help='URL of the CSV file')

    args = parser.parse_args()
    main(args)


