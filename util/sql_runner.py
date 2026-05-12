import pandas as pd
from sqlalchemy import text


def execute_sql_and_read_temp_table(engine, setup_sql: str, table_name: str):
    query = f"SELECT * FROM {table_name}"
    with engine.connect() as conn:
        conn.exec_driver_sql(setup_sql)
        return pd.read_sql_query(text(query), conn)


def execute_sql_and_read_result_sets(engine, setup_sql: str, max_sets: int | None = None) -> list[pd.DataFrame]:
    """Execute a SQL batch and return each result set as a DataFrame.

    Useful for scripts that intentionally end with multiple SELECT statements,
    such as the payment-attempt extract returning normal, arrangement, and
    third-party streams.
    """
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute(setup_sql)

        result_sets: list[pd.DataFrame] = []
        while True:
            if cursor.description is not None:
                columns = [col[0] for col in cursor.description]
                rows = [tuple(row) for row in cursor.fetchall()]
                result_sets.append(pd.DataFrame.from_records(rows, columns=columns))
                if max_sets is not None and len(result_sets) >= max_sets:
                    break

            if not cursor.nextset():
                break

        return result_sets
    finally:
        raw_conn.close()

