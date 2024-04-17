import pandas as pd
from sqlalchemy import create_engine, text
from time import time
import argparse
import os 


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    csv_name = 'output.csv.gz'
    
    #connect to database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    os.system(f'wget {url} -O {csv_name}') 
    
    df = pd.read_csv(csv_name, compression='gzip', low_memory=False)
    
    #conerting data types , this is not necessary ASwe will do this later whle ingesting
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    print(' ')
    print(f'Ingesting data into postgresql://{user}:{password}@{host}:{port}/{db}')
    print('using the following schema:')
    print(pd.io.sql.get_schema(df, name=table, con=engine))
    
    #create a empty table in database
    df.head(0).to_sql(con=engine, name=table, if_exists='replace')
    
    #ingesting data in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)
    while True:
        try:
            t_start = time()
            
            df=next(df_iter)
            
            # fix the type of these two fields. Shuold be datetime
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(con = engine, name = table, if_exists = 'append')
            
            t_end = time()
    
            print('inserted another chunk... it took %.3f seconds' % (t_end - t_start))
            
        except StopIteration:
            print('All rows ingested')
            break

    
if __name__== '__main__':
    parser = argparse.ArgumentParser(description='Ingest yellow taxi data csv to postgres')
     
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table')
    parser.add_argument('--url', help='url for csv file')
    
    args = parser.parse_args()
    
    main(args)
# we will run the docker with arguments and pass those arguments in main function