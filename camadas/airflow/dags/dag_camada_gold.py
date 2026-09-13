from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


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
    dag_id='pipeline_transformacao_gold',
    default_args=default_args,
    description='Transformação dos dados da camada Silver em tabelas Gold',
    schedule='@weekly',
    catchup=False,
    tags=['synthesus', 'gold', 'oci', 'docker'],
) as dag:

    processar_camada_gold = DockerOperator(
        task_id='disparar_transformacao_gold',

        # Imagem criada com:
        # docker build -t synthesus-gold:latest .
        image='synthesus-gold:latest',

        api_version='auto',

        # Remove o container depois da execução
        auto_remove='force',

        # Script executado dentro do container
        command='python script_transformacao_gold.py',

        # Permite que o Airflow controle o Docker do host
        docker_url='unix://var/run/docker.sock',

        network_mode='bridge',

        # Limite de memória do processamento Gold
        mem_limit='16g',

        # Evita criação automática de volume temporário
        mount_tmp_dir=False,

        # Tempo máximo para a transformação Gold
        execution_timeout=timedelta(minutes=30),

        environment={
            'PYTHONUNBUFFERED': '1'
        },

        # Se for necessário montar a configuração OCI:
        #
        # mounts=[
        #     Mount(
        #         source='/opt/airflow/config',
        #         target='/app/config',
        #         type='bind'
        #     )
        # ]
    )

    processar_camada_gold