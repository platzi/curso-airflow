from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, date

default_args = {
'start_date': datetime(2022, 7, 1),
'end_date': datetime(2022, 8, 1)
}

def _choose(**context):

    if context["logical_date"].date() < date(2022, 7, 15):
        return "finish_14_june"

    return "start_15_june"

with DAG('10-branching',
         schedule_interval='@daily',
         default_args=default_args) as dag:

    branching = BranchPythonOperator(task_id="branch",
                                     python_callable=_choose)

    finish_14 = BashOperator(task_id='finish_14_june',
                            bash_command="echo 'Running {{ds}}'")

    start_15 = BashOperator(task_id='start_15_june',
                              bash_command="echo 'Running {{ds}}'")

    branching >> [finish_14, start_15]