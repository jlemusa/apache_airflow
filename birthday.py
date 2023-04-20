from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


def get_day_to_next_birthday():
    birthday = datetime(year=1993, month=3, day=18)
    today = datetime.now()
    next_birthday = datetime(year=today.year, month=birthday.month, day=birthday.day)
    if today > next_birthday:
        next_birthday = next_birthday.replace(year=today.year + 1)
    days_to_birthday = (next_birthday - today).days
    print(f"{days_to_birthday} missing for your next birthday!")


with DAG(dag_id="birthday_calculator", start_date=datetime(2023, 1, 1), schedule_interval="@once", catchup=False):

    print_actual_date = BashOperator(
        task_id="print_actual_date",
        bash_command="date",
    )

    print_days_to_birthday = PythonOperator(
        task_id="print_days_to_birthday",
        python_callable=get_day_to_next_birthday,
    )

    print_actual_date >> print_days_to_birthday


    
