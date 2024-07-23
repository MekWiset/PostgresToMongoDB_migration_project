import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import os
from dotenv import load_dotenv
from pathlib import Path

from plugins.extract.postgres_extractor import extract_postgres
from plugins.transform.data_transformer import final_transformation
from plugins.load.mongo_loader import upload_to_mongodb
from dags.helpers.sql_query import MEDQ_QUERY


dotenv_path = Path('.env')
load_dotenv(dotenv_path = dotenv_path)

# Variables
PG_CONN = {
    'host': os.getenv('MEDQ_HOST'),
    'dbname': os.getenv('MEDQ_DBNAME'),
    'user': os.getenv('MEDQ_USER'),
    'password': os.getenv('MEDQ_PASSWORD')
}
MONGO_CLIENT = os.getenv('MONGO_CIENT')

raw_medq_path = 'data/medq_data.csv'
hospital_data_path = 'data/ref_hospital.xlsx'
transformed_medq_path = 'data/medq_data_transformed.csv'


default_args = {
    'owner': 'Mek',
    'retries': 1,
    'retry_delay': timedelta(seconds=3)
}

with DAG(

    default_args = default_args,
    dag_id = 'postgres_to_mongodb',
    description = 'DAG to process and export MedQ data from Postgres database to MongoDB',
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = None,
    catchup = False

)as dag:

    extract_postgres_task = PythonOperator(
        task_id = 'extract_medq_data',
        python_callable = extract_postgres,
        op_kwargs = {
            'pg_conn': PG_CONN,
            'query': MEDQ_QUERY,
            'output_path': raw_medq_path
            }
    )

    transform_data_task = PythonOperator(
        task_id = 'transform_medq_data',
        python_callable = final_transformation,
        op_kwargs = {
            'medq_path': raw_medq_path,
            'hospital_path': hospital_data_path,
            'output_path': transformed_medq_path
            }
    )

    upload_to_mongo_task = PythonOperator(
        task_id = 'upload_to_mongodb',
        python_callable = upload_to_mongodb,
        op_kwargs = {
            'file_path': transformed_medq_path,
            'client_uri': MONGO_CLIENT,
            'dbname': 'HDX',
            'collection_name': 'RMCPLUS_HDX_PRODUCT'
        }
    )


    # Task Dependencies
    extract_postgres_task >> transform_data_task >> upload_to_mongo_task