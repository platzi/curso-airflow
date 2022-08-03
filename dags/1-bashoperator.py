from airflow import  DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="bashoperator",
         description="Nuestro primer DAG hecho con bash",
         start_date=datetime(2022, 8, 1)) as dag:

    t1 = BashOperator(task_id="tarea1",
                      bash_command="echo 'Hola gente de Platzi!'")
    t1
