from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(dag_id="Retrieving_Users", schedule_interval="@once", start_date=datetime(2023, 1, 1), catchup=False):

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
        command=f"grep -e gender -e first -e last /tmp/user01.json | tr -s ' ' | tr -d '^ ' > /ProcessedFiles/user{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
    )

    remove_tmp_files = SSHOperator(
        task_id="remove_tmp_files",
        ssh_conn_id="SSH_to_SV02",
        command="rm -f /tmp/user.json /tmp/user.json,tmp",
    )

    get_user_info >> parse_json_file >> extract_user_info >> remove_tmp_files

