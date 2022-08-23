from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

def myfunction():
    raise Exception


with DAG(dag_id="6.2-monitoring",
         description="Monitoreando nuestro DAG",
         schedule_interval="@daily",
         start_date=datetime(2022, 1, 1),
         end_date=datetime(2022, 6, 1),
         max_active_runs=1) as dag:


    t1 = BashOperator(task_id="tarea1",
                      bash_command="sleep 5 && echo 'Primera tarea!'",
                      trigger_rule=TriggerRule.ALL_SUCCESS,
                      retries=2,
                      retry_delay=5,
                      depends_on_past=False)


    t2 = BashOperator(task_id="tarea2",
                      bash_command="sleep 3 && echo 'Segunda tarea!'",
                      retries=2,
                      retry_delay=5,
                      trigger_rule=TriggerRule.ALL_SUCCESS,
                      depends_on_past=True)

    t3 = BashOperator(task_id="tarea3",
                      bash_command="sleep 2 && echo 'Tercera tarea!'",
                      retries=2,
                      retry_delay=5,
                      trigger_rule=TriggerRule.ALWAYS,
                      depends_on_past=True)

    t4 = PythonOperator(task_id="tarea4",
                        python_callable= myfunction,
                        retries=2,
                        retry_delay=5,
                      trigger_rule=TriggerRule.ALL_SUCCESS,
                      depends_on_past=True)

    t5 = BashOperator(task_id="tarea5",
                      bash_command="sleep 2 && echo 'Cuarta tarea!'",
                      retries=2,
                      retry_delay=5,
                      depends_on_past=True)

    t1 >> t2 >> t3 >> t4 >> t5