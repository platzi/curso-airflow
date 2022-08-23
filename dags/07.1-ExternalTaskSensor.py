from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(dag_id="7.1-externalTaskSensor",
         description="DAG principal",
         schedule_interval="@daily",
         start_date=datetime(2022, 5, 1),
         end_date=datetime(2022, 8, 1)) as dag:

    t1 = BashOperator(task_id="tarea1",
                      bash_command="sleep 10 && echo 'DAG finalizado!'",
                      depends_on_past=True)
    
    t1