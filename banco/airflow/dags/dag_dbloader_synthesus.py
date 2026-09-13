from datetime import timedelta

import pendulum
from docker.types import Mount

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


IMAGE = "synthesus-dbloader:latest"
DOCKER_URL = "unix://var/run/docker.sock"

OCI_VOLUME = "synthesus-dbloader-oci"
WALLET_VOLUME = "synthesus-dbloader-wallet"

DBLOADER_ENV_FILE = "/opt/synthesus/dbloader.env"


MOUNTS_DBLOADER = [
    Mount(
        source=OCI_VOLUME,
        target="/root/.oci",
        type="volume",
        read_only=True,
    ),
    Mount(
        source=WALLET_VOLUME,
        target="/opt/oracle/wallet",
        type="volume",
        read_only=True,
    ),
]


def criar_task_dbloader(
    task_id,
    argumentos,
    retries=2,
    execution_timeout=timedelta(hours=8),
):
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        command=[
            "python",
            "src/main.py",
            *argumentos,
        ],
        docker_url=DOCKER_URL,
        api_version="auto",
        force_pull=False,
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=MOUNTS_DBLOADER,
        env_file=DBLOADER_ENV_FILE,
        environment={
            "PYTHONUNBUFFERED": "1",
        },
        network_mode="bridge",
        do_xcom_push=False,
        tty=False,
        retries=retries,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=execution_timeout,
    )


with DAG(
    dag_id="pipeline_carga_synthesus_oracle",
    description=(
        "Sincroniza a camada Gold do SyntheSUS com o Oracle "
        "usando carga inteligente por ETag/competência."
    ),
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="America/Sao_Paulo",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=[
        "synthesus",
        "gold",
        "oracle",
        "dbloader",
        "docker",
    ],
) as dag:

    dim_data = criar_task_dbloader(
        task_id="carga_dim_data",
        argumentos=[
            "--table",
            "DIM_DATA",
        ],
        execution_timeout=timedelta(hours=1),
    )

    dim_municipio = criar_task_dbloader(
        task_id="carga_dim_municipio",
        argumentos=[
            "--table",
            "DIM_MUNICIPIO",
        ],
        execution_timeout=timedelta(hours=1),
    )

    unidade_saude = criar_task_dbloader(
        task_id="carga_unidade_saude",
        argumentos=[
            "--table",
            "UNIDADE_SAUDE",
        ],
    )

    alvara = criar_task_dbloader(
        task_id="carga_alvara",
        argumentos=[
            "--table",
            "ALVARA",
        ],
    )

    profissional = criar_task_dbloader(
        task_id="carga_profissional",
        argumentos=[
            "--table",
            "PROFISSIONAL",
        ],
        execution_timeout=timedelta(hours=12),
    )

    fato_leitos = criar_task_dbloader(
        task_id="carga_fato_leitos",
        argumentos=[
            "--table",
            "FATO_LEITOS",
        ],
    )

    fato_equipamentos = criar_task_dbloader(
        task_id="carga_fato_equipamentos",
        argumentos=[
            "--table",
            "FATO_EQUIPAMENTOS",
        ],
        execution_timeout=timedelta(hours=10),
    )

    internacao = criar_task_dbloader(
        task_id="carga_internacao",
        argumentos=[
            "--table",
            "INTERNACAO",
        ],
        execution_timeout=timedelta(hours=10),
    )

    cleanup_retention = criar_task_dbloader(
        task_id="cleanup_retencao_12_meses",
        argumentos=[
            "--cleanup-retention",
        ],
        retries=1,
        execution_timeout=timedelta(hours=4),
    )

    [
        dim_data,
        dim_municipio,
    ] >> unidade_saude

    unidade_saude >> [
        alvara,
        profissional,
        fato_leitos,
        fato_equipamentos,
        internacao,
    ]

    [
        alvara,
        profissional,
        fato_leitos,
        fato_equipamentos,
        internacao,
    ] >> cleanup_retention