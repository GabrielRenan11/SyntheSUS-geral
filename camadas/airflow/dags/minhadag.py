from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="minha_primeira_dag",
    start_date=datetime(2026, 7, 7),
    schedule="@daily",
    catchup=False,  # Evita acumular execuções dos dias passados
) as dag:
    
    inicio = EmptyOperator(task_id="inicio")
    
    # Corrigido de Bash_command para bash_command (tudo em minúsculo)
    hello = BashOperator(task_id="hello", bash_command="echo 'hello world'")
    
    fim = EmptyOperator(task_id="fim")

    inicio >> hello >> fim