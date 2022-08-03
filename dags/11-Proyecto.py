import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor


def _generate_platzi_data(**kwargs):
    import pandas as pd
    data = pd.DataFrame({"student": ["Maria Cruz", "Daniel Crema", "Elon Musk", "Karol Castrejon", "Freddy Vega"],
                         "timestamp": [kwargs['logical_date'], kwargs['logical_date'], kwargs['logical_date'], kwargs['logical_date'], kwargs['logical_date']]})

    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv", header=True)

def _notify_datateam():
    print("¡Hola equipo de data! ¡Los datos del {{ds}} se encuentran disponibles!")

default_args={
    'depends_on_past': True
}

with DAG(dag_id="proyecto_platzi",
         description="Proyecto de Platzi",
         schedule_interval="@monthly",
         start_date=datetime(2022, 1, 1),
         end_date=datetime.today().replace(day=1),
         max_active_runs=1,
         default_args=default_args) as dag:

    task1 = BashOperator(
        task_id="receiving_nasa_confirmation",
        bash_command='sleep 20 && echo "OK" > /tmp/response_{{ds_nodash}}.txt'
    )

    # RECUERDA CREAR LA CONEXION
    sensor1_task = FileSensor(task_id= "waiting_nasa_confirmation",
                              poke_interval= 30,  filepath= "/tmp/response_{{ds_nodash}}.txt" )

    task2 = BashOperator(task_id="get_spacex_data",
                      bash_command="curl -o /tmp/history.json -L 'https://api.spacexdata.com/v4/history'")

    task3 = PythonOperator(task_id="generating_platzi_data",
                        python_callable=_generate_platzi_data)

    task4  = BashOperator(
        task_id="print_logs",
        bash_command='ls /tmp && head /tmp/platzi_data.csv'
    )

    task5  = BashOperator(
        task_id="notify_marketing_team",
        bash_command='echo "¡Hola equipo de marketing! ¡Los datos del {{ds}} se encuentran disponibles!"'
    )

    # CHALLENGE CON CUSTOM OPERATOR
    task6 = PythonOperator(task_id="notify_data_team",
                           python_callable=_notify_datateam)

    task1 >> sensor1_task >> task2 >> task3 >> task4 >> [task5, task6]