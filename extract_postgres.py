import logging
import os
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Configurar logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Directory created: {directory}")
    else:
        logger.info(f"Directory already exists: {directory}")

def create_evidence_file(output_log, message):
    clean_directory(os.path.dirname(output_log))
    try:
        with open(output_log, 'w') as f:
            f.write(message)
        logger.info(f"Evidence file created at {output_log}")
    except Exception as e:
        logger.error(f"Failed to create evidence file. Reason: {e}")

def extract_from_postgres(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d_%H-%M-%S')
    db_url = "postgresql://northwind_user:thewindisblowing@db/northwind"

    try:
        engine = create_engine(db_url)
        logger.info("Connected to PostgreSQL database")

        tables = engine.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()
        tables = [table[0] for table in tables]

        for table in tables:
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, engine)
            logger.info(f"Query executed successfully for table {table}. Number of rows returned: {len(df)}")

            output_dir = f"/opt/airflow/data/output/postgres/{table}/{execution_date}"
            clean_directory(output_dir)
            output_path = os.path.join(output_dir, "data.csv")
            df.to_csv(output_path, index=False)
            logger.info(f"Data saved successfully to: {output_path}")

            output_log = os.path.join(output_dir, "extraction_log.json")
            create_evidence_file(output_log, f"Data extraction completed for table {table} on {execution_date}\n")

    except Exception as e:
        logger.error("Error extracting data from PostgreSQL", exc_info=True)
        raise e

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_postgres',
    default_args=default_args,
    description='Extract data from PostgreSQL',
    schedule_interval='@daily',
    catchup=True,
)

extract_task = PythonOperator(
    task_id='extract_from_postgres',
    provide_context=True,
    python_callable=extract_from_postgres,
    dag=dag,
)
