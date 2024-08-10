import connection
import os
import sqlparse
import pandas as pd

if __name__ == '__main__':
    #connection data source
    conf = connection.config('marketplace_prod')
    conn,engine = connection.get_conn(conf,"DataSource")
    
    #connection dwh
    conf_dwh =connection.config('dwh') 
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh,'DWH')
    cursor_dwh = conn.cursor()
    
    #qet query string
    path_query =  os.getcwd()+'/query/'
    query = sqlparse.format(
        open(path_query + 'query.sql','r').read(), strip_commnets=True
    ).strip()
    dwh_desain = sqlparse.format(
        open(path_query + 'dwh_desain.sql','r').read(), strip_commnets=True
    ).strip()
    
    #get data
    try:
        print('[info] service etl is running..')
        df = pd.read_sql(query, engine)
    
    #create schema dwh
        cursor_dwh.execute(dwh_desain)
        conn_dwh.commit()
        
    #import data
        df.to_sql(
            'dim_orders_qori',
            engine_dwh,
            schema='public',
            if_exists='replace',
            index=False
        )
        print('[INFO] service etl is success...')
    
    
    #ingest data to dwh
    except Exception as e:
        print('[INFO] service etl is failed')
        print(str(e))