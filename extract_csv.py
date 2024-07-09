import logging
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Configurar logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Função para executar comandos no container Docker do meltano
def run_meltano_command(command, execution_date):
    container_id = "085eef70b19f"  # Substitua pelo ID do seu container meltano
    try:
        subprocess.run([
            "docker", "exec", container_id, "sh", "-c", command
        ], check=True, env={"MELTANO_RUN_DATETIME": execution_date})
        logger.info(f"Meltano command '{command}' completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to run Meltano command '{command}'. Reason: {e}")

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

def extract_from_csv(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d_%H-%M-%S')
    output_dir = f"/opt/airflow/data/output/csv/{execution_date}"
    clean_directory(output_dir)

    try:
        run_meltano_command(
            "meltano elt tap-csv target-csv --job_id=extract_csv " +
            f"--output_dir={output_dir}", execution_date
        )
        logger.info("CSV extraction completed")

        output_log = os.path.join(output_dir, "extraction_log.json")
        create_evidence_file(output_log, f"CSV extraction completed on {execution_date}\n")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to extract data from CSV. Reason: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_csv',
    default_args=default_args,
    description='Extract data from CSV',
    schedule_interval='@daily',
    catchup=True,
)

extract_task = PythonOperator(
    task_id='extract_from_csv',
    provide_context=True,
    python_callable=extract_from_csv,
    dag=dag,
)
