from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


templated_command = """
{% for file in params.filenames %}
    echo "{{ ds }}"
    echo "{{ file }}"
{% endfor %}

"""

with DAG(dag_id="8-Templating",
         description="Example using templates",
         schedule_interval="@daily",
         start_date=datetime(2022, 7, 1),
         end_date=datetime(2022, 8, 1),
         max_active_runs=1) as dag:

    t1 = BashOperator(task_id="tarea1",
                      bash_command=templated_command,
                      params={"filenames": ["file1.txt", "file2.txt"]},
                      depends_on_past=True)

    t1
