import logging
import os
import pandas as pd
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configurar logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Limpeza de diretório
def clean_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Directory created: {directory}")
    else:
        logger.info(f"Directory already exists: {directory}")

# Criar arquivo de evidência
def create_evidence_file(output_log, message):
    clean_directory(os.path.dirname(output_log))
    try:
        with open(output_log, 'w') as f:
            f.write(message)
        logger.info(f"Evidence file created at {output_log}")
    except Exception as e:
        logger.error(f"Failed to create evidence file. Reason: {e}")

# Carregar dados para o PostgreSQL
def load_to_postgres(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d_%H-%M-%S')
    input_csv = "/opt/airflow/data/order_details.csv"
    output_dir = f"/opt/airflow/data/output/csv/{execution_date}"
    
    try:
        # Leitura dos dados do arquivo CSV
        df_csv = pd.read_csv(input_csv)
        logger.info(f"CSV data loaded successfully. Number of rows: {len(df_csv)}")
        
        # Execução de subprocesso para carregar CSV no PostgreSQL usando Meltano
        subprocess.run([
            "meltano", "elt", "tap-csv", "target-postgres", "--job_id=load_postgres",
            f"--input_dir={output_dir}"
        ], check=True, env={"MELTANO_RUN_DATETIME": execution_date})
        logger.info("Data loaded into PostgreSQL")

        # Criação de arquivo de evidência
        output_log = f"/opt/airflow/data/logs/{execution_date}_load_to_postgres.json"
        create_evidence_file(output_log, f"Data load completed for {execution_date}\n")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to load data into PostgreSQL. Reason: {e}")

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Criar a DAG
dag = DAG(
    'load_postgres',
    default_args=default_args,
    description='Load data into PostgreSQL',
    schedule_interval='@daily',
    catchup=True,
)

# Tarefa de PythonOperator para carregar dados para o PostgreSQL
load_task = PythonOperator(
    task_id='load_to_postgres',
    provide_context=True,
    python_callable=load_to_postgres,
    dag=dag,
)
