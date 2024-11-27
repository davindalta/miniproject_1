import os
import connection
import sqlparse
import pandas as pd

if __name__ == "__main__":
    # connection source
    conf = connection.config("local", "config.local.json")
    conn, engine = connection.get_conn(conf, "source")
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS dwh;")
    conn.commit()

    # connection dwh
    conf_dwh = connection.config("local_dwh", "config.local.json")
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, "dwh")
    cursor_dwh = conn_dwh.cursor()

    # get query string
    path_query = os.path.join(os.getcwd(), "query")

    query = sqlparse.format(
        open(os.path.join(path_query, "query.sql"), "r").read(), strip_comments=True
    ).strip()
    dwh_design = sqlparse.format(
        open(os.path.join(path_query, "dwh_design.sql"), "r").read(),
        strip_comments=True,
    ).strip()

    try:
        print("[INFO] service etl is running...")

        # get data from source (schema = public, if your source schema is not public, then adjust the query to use with your source schema)
        df = pd.read_sql(query, engine)
        print(df)

        # insert the schema into dwh schema
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        # ingest data into dwh schema
        df.to_sql(
            "dim_orders", engine_dwh, schema="dwh", if_exists="replace", index=False
        )

        print("[INFO] service etl is success")
    except Exception as e:
        print("[INFO] service etl is failed")
        print(str(e))
