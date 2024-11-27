import os
import json
import psycopg2
import sqlalchemy


def config(connection_db, file_name):
    path = os.getcwd()
    with open(os.path.join(path, file_name)) as file:
        conf = json.load(file)[connection_db]

    return conf


def get_conn(conf, name_conn):
    try:
        conn = psycopg2.connect(
            host=conf["host"],
            database=conf["db"],
            user=conf["user"],
            password=conf["password"],
            port=conf["port"],
        )

        print(f"[INFO] success connect postgres {name_conn}")

        connect_args = (
            {"options": "-csearch_path={}".format(conf["schema"])}
            if "schema" in conf
            else {}
        )

        engine = sqlalchemy.create_engine(
            "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                conf["user"],
                conf["password"],
                conf["host"],
                conf["port"],
                conf["db"],
            ),
            connect_args=connect_args,
        )

        return conn, engine

    except Exception as e:
        print(f"[ERROR] can't connect to postgres {name_conn}")
        print(str(e))
