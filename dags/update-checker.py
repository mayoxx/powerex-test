import requests
import pendulum
import hashlib
import logging
import json

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def check():
    url = "https://api.open-meteo.com/v1/ecmwf?latitude=48.1482&longitude=17.1067&hourly=temperature_2m,relativehumidity_2m,rain,windspeed_10m,winddirection_10m"
    response = requests.request("GET", url)
    json_data = response.json()
    json_data['generationtime_ms'] = 0
    new_hash = hashlib.md5(json.dumps(json_data).encode("utf-8")).hexdigest()

    old_hash = Variable.get('open-meteo-ecmwf-data-hash', default_var=0)

    logging.info('OLD HASH: ' + old_hash)
    logging.info('NEW HASH: ' + new_hash)

    if old_hash != new_hash:
        logging.info('HASHES DIFFER, TRIGGERED UPDATE')
        Variable.set(key='open-meteo-ecmwf-data-hash', value=new_hash)
        return "trigger_dag_update"
    else:
        return 'nothing_to_do'

@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    #schedule_interval='*/1 * * * *',
    tags=["update"]
)

def update_checker():
    @task()
    def nothing_to_do():
        logging.info('HASHES DON\'T DIFFER, UPDATE NOT TRIGGERED')

    trigger_dag_update = TriggerDagRunOperator(
        task_id='trigger_dag_update',
        trigger_dag_id="open_meteo_running_avgs"
    )
    new_data_exist = BranchPythonOperator(
        task_id='new_data_exist',
        python_callable=check
    )
    
    new_data_exist >> [trigger_dag_update, nothing_to_do()]

update_checker()
