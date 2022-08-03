from datetime import datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator


templated_command = dedent(
    """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
{% endfor %}
sleep 10
"""
)

with DAG(dag_id="7-Templating",
         description="Example using templates",
         schedule_interval="@daily",
         start_date=datetime(2022, 5, 1),
         end_date=datetime(2022, 8, 1),
         max_active_runs=1) as dag:

    t1 = BashOperator(task_id="tarea1",
                      bash_command=templated_command,
                      depends_on_past=True)

    t1
