from airflow import DAG
from datetime import datetime
from hellooperator import HelloOperator

with DAG(dag_id="customoperator",
         description="Nuestro primer customoperator",
         start_date=datetime(2022, 8, 1)) as dag:

    t1 = HelloOperator(task_id="hello",
                       name="Freddy")

    t1