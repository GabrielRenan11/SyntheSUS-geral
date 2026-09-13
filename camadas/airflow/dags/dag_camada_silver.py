from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'synthesus',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_tranformacao_silver',
    default_args=default_args,
    description='Transformação dos dados brutos da Bronze em tabelas Silver',
    schedule='@weekly',
    catchup=False,
    tags=['synthesus', 'silver', 'oci', 'docker'],
) as dag:

    processar_camada_silver = DockerOperator(
        task_id='disparar_transformacao_silver',
        image='synthesus-transformacao:latest',
        api_version='auto',
        auto_remove='force',  # <--- Alinhado para 'force' para evitar conflito 409 em retries
        command='python script_transformacao_silver.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mem_limit='16g',
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=60),
        environment={
            'PYTHONUNBUFFERED': '1'
        },
        #mounts=[
        #    Mount(
        #        source='/opt/airflow/config',  # Verifique se o caminho existe no Host
        #        target='/app/config',
        #        type='bind'
        #    )
        #]
    )

    processar_camada_silver