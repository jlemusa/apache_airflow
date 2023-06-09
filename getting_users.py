from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime
from mySensors import RemoteFileSensor

with DAG(dag_id="Retrieving_Users", schedule_interval="@once", start_date=datetime(2023, 1, 1), catchup=False):

    filename = f"/ProcessedFiles/user{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"

    ssh_hook = SSHHook(ssh_conn_id="SSH_to_SV02")

    get_user_info = SSHOperator(
        task_id="get_json",
        ssh_conn_id="SSH_to_SV02",
        command="curl -X GET -H 'Content-Type: application/json' 'https://randomuser.me/api/' > /tmp/user.json.tmp"
    )

    parse_json_file = SSHOperator(
        task_id="parse_json_file",
        ssh_conn_id="SSH_to_SV02",
        command="jq '.' /tmp/user.json.tmp > /tmp/user.json"
    )

    extract_user_info = SSHOperator(
        task_id="extract_user_info",
        ssh_conn_id="SSH_to_SV02",
        command=f"grep -e gender -e first -e last /tmp/user.json | tr -s ' ' | tr -d '^ ' > {filename}"
    )

    remove_tmp_files = SSHOperator(
        task_id="remove_tmp_files",
        ssh_conn_id="SSH_to_SV02",
        command="rm -f /tmp/user.json /tmp/user.json.tmp",
    )

    check_file = RemoteFileSensor(
        task_id="check_file",
        filepath="/tmp/user.json",
        hostname=ssh_hook.remote_host,
        username=ssh_hook.username,
        password=ssh_hook.password
    )

    get_user_info >> parse_json_file >> check_file >> extract_user_info >> remove_tmp_files

