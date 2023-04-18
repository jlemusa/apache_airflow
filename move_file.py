from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(dag_id="moving_files", start_date=datetime(2023, 1, 1), schedule_interval="@once"):
    list_files_and_log = SSHOperator(task_id="list_files_and_log", command=f"find /Files/ -type f -iname file* > /LogProcess/files_to_move", ssh_conn_id="SSH_to_SV02")
    move_files = SSHOperator(task_id="move_files", command="mv $(cat /LogProcess/files_to_move) /ProcessedFiles/", ssh_conn_id="SSH_to_SV02")
    remove_temp_file = SSHOperator(task_id="remove_temp_file", command="rm /LogProcess/files_to_move", ssh_conn_id="SSH_to_SV02")

    list_files_and_log >> move_files >> remove_temp_file