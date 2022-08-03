from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="6.2-externalTaskSensor",
         description="DAG Secundario",
         schedule_interval="@daily",
         start_date=datetime(2022, 5, 1),
         end_date=datetime(2022, 8, 1),
         max_active_runs=1) as dag:

    t1 = ExternalTaskSensor(task_id="waiting_task",
                            external_dag_id="6.1-externalTaskSensor",
                            external_task_id="tarea1",
                            depends_on_past=True,
                            poke_interval=15)

    t2 = BashOperator(task_id="tarea2",
                      bash_command="sleep 20 && echo 'DAG 2 finalizado!'",
                      depends_on_past=True)
    t1 >> t2