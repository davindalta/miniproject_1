import os
import json
import psycopg2
from sqlalchemy import create_engine

# func config
def config(connection_db):
    path = os.getcwd()
    with open(os.path.join(path, 'config.json')) as file:
        conf = json.load(file)[connection_db]
    return conf

# try connection
# print(config('marketplace_prod'))


# func get_conn
def get_conn(conf, name_conn):
    try:
        conn=psycopg2.connect(
            host=conf['host'],
            database=conf['db'],
            user=conf['user'],
            password=conf['password'],
            port=conf['port']
        )
        print(f'[INFO] success connect postgres {name_conn}')
        
        engine = create_engine(
            "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                conf['user'],
                conf['password'],
                conf['host'],
                conf['port'],
                conf['db']
            )
        )
        return conn, engine
    except Exception as e:
        print(f"[ERROR] can't success connect to postgres {name_conn}")
        print(str(e))