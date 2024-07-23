import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import os

from dags.helpers.sql_query import MEDQ_QUERY


def extract_postgres(pg_conn: dict, query: str, output_path: str) -> None:
    '''Extract MedQ data from Postgres database through API using psycopg2'''
    conn = psycopg2.connect(**pg_conn)
    
    query = query
    df_medq = pd.read_sql(query, conn)

    df_medq.to_csv(output_path)

    conn.close()


if __name__ == '__main__':

    dotenv_path = Path('.env')
    load_dotenv(dotenv_path = dotenv_path)

    PG_CONN = {
        'host': os.getenv('MEDQ_HOST'),
        'dbname': os.getenv('MEDQ_DBNAME'),
        'user': os.getenv('MEDQ_USER'),
        'password': os.getenv('MEDQ_PASSWORD')
    }

    output_path = 'data/medq_data.csv'

    extract_postgres(pg_conn=PG_CONN, query=MEDQ_QUERY, output_path=output_path)

