from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2022, 1, 1)
}

with DAG(dag_id="move_files_on_sv02", default_args=default_args, schedule_interval="@daily"):
    move_file = SSHOperator(
        task_id="move_file",
        ssh_conn_id="SSH_to_SV02",
        command="mv /Files/* /ProcessedFiles"
    )

    finish_task = BashOperator(
        task_id="finish_task",
        bash_command="Done!",
    )

move_file > finish_task