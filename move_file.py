from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(dag_id="my_dag", start_date=datetime(2023, 1, 1), schedule_interval="@once"):
    run_bash_command = SSHOperator(task_id="run_bash_command", command="ls -al /Files", ssh_conn_id="SSH_to_SV02")

run_bash_command