from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(dag_id="moving_files", start_date=datetime(2023, 1, 1), schedule_interval="@once"):
    list_files_and_log = SSHOperator(task_id="list_files_and_log", command=f"ls -al /Files > /LogProcess/log-{datetime.now().strftime('%Y%m%d')}", ssh_conn_id="SSH_to_SV02")
    move_files = SSHOperator(task_id="move_files", command="mv /Files/* /ProcessedFiles/", ssh_conn_id="SSH_to_SV02")

    list_files_and_log 