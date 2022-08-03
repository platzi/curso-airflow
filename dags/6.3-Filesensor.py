from datetime import datetime
from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

with DAG(dag_id="6.3-filesensor",
         description="FileSensor",
         schedule_interval="@daily",
         start_date=datetime(2022, 5, 1),
         end_date=datetime(2022, 8, 1),
         max_active_runs=1) as dag:

    t1 = BashOperator(task_id="tarea2",
                      bash_command="sleep 10 && touch /tmp/file.txt",
                      depends_on_past=True)

    t2= FileSensor(task_id="filesensor_task",
                    filepath = "/tmp/file.txt",
                    depends_on_past=True,
                    poke_interval=15)

    t3 = BashOperator(task_id="tarea3",
                      bash_command="rm /tmp/file.txt",
                      depends_on_past=True)
    t1 >> t2 >> t3