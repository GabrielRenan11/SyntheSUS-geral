from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "gabriel.silva",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pipeline_ingestao_synthesus_bronze",
    default_args=default_args,
    description="Orquestra o container Docker de ingestão do SIM SP 2024 para o Object Storage da Oracle",
    schedule="@monthly",  # Executa uma vez por mês automaticamente
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["synthesus", "bronze", "datasus"],
) as dag:

    executar_ingestao_container = DockerOperator(
        task_id="disparar_container_ingestao",
        image="synthesus-ingestao:latest",
        api_version="auto",
        auto_remove="force",
        command="python ingestao_synthesus.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,  # Remove o aviso amarelo do log
        execution_timeout=timedelta(hours=12),  # Corrigido aqui
        environment={
            "PYTHONUNBUFFERED": "1"  # Força o Python a mandar os prints pro log na hora
        },
    )

    executar_ingestao_container
