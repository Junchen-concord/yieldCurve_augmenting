import pandas as pd
from sqlalchemy import text


def execute_sql_and_read_temp_table(engine, setup_sql: str, table_name: str):
    query = f"SELECT * FROM {table_name}"
    with engine.connect() as conn:
        conn.exec_driver_sql(setup_sql)
        return pd.read_sql_query(text(query), conn)

