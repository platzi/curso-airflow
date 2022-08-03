from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date

default_args = {
'start_date': datetime(2022, 7, 1),
'end_date': datetime(2022, 8, 1)
}
def _choose_function(**context):

    if context["logical_date"].date() < date(2022, 7, 15):
        return 'finish_14_june'

    return 'start_15_june'

with DAG('branching',
         schedule_interval='@daily',
         default_args=default_args) as dag:

    choose_function = BranchPythonOperator(
            task_id='choose_function',
            python_callable=_choose_function
    )

    finish_14 = BashOperator(task_id='finish_14_june',
                            bash_command="echo 'Running {{ds}}'")

    start_15 = BashOperator(task_id='start_15_june',
                              bash_command="echo 'Running {{ds}}'")

    choose_function >> [finish_14, start_15]