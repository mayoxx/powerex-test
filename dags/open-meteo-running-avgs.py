import requests
import pandas as pd
import logging
import json
import pendulum
import os
import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def get_last_7_days():
    sql_stmt = "SELECT * FROM ecmwf WHERE time > (CURRENT_TIMESTAMP - INTERVAL '7 days') AND time < CURRENT_TIMESTAMP ORDER BY time ASC;"
    pg_hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()
    
def get_forecast():
    sql_stmt = "SELECT * FROM ecmwf WHERE time > CURRENT_TIMESTAMP ORDER BY time ASC;"
    pg_hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

def get_meteo_db():
    sql_stmt = "SELECT * FROM meteo;"
    pg_hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()
    
@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    #schedule_interval='0 */6 * * *',
    tags=["ingestion"]
)

def open_meteo_running_avgs():
    DB_ID = "tutorial_pg_conn"

    create_ecmwf_table = PostgresOperator(
        task_id="create_ecmwf_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS ecmwf;
            CREATE TABLE IF NOT EXISTS ecmwf (
                "id" BIGINT PRIMARY KEY,
                "time" TIMESTAMPTZ,
                "temperature_2m" REAL,
                "relativehumidity_2m" INTEGER,
                "rain" REAL,
                "windspeed_10m" REAL,
                "winddirection_10m" INTEGER
            );""",
    )

    create_meteo_table = PostgresOperator(
        task_id="create_meteo_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS meteo;
            CREATE TABLE meteo (
                "id" NUMERIC PRIMARY KEY,
                "time" TIMESTAMP,

                "actual_temperature" DOUBLE PRECISION,
                "mean_7_days_temperature" DOUBLE PRECISION,
                "std_7_days_temperature" DOUBLE PRECISION,

                "actual_humidity" INTEGER,
                "mean_7_days_humidity" INTEGER,
                "std_7_days_humidity" INTEGER,

                "actual_rain" DOUBLE PRECISION,
                "mean_7_days_rain" DOUBLE PRECISION,
                "std_7_days_rain" DOUBLE PRECISION,

                "actual_windspeed_10m" DOUBLE PRECISION,
                "mean_7_days_windspeed_10m" DOUBLE PRECISION,
                "std_7_days_windspeed_10m" DOUBLE PRECISION,

                "actual_winddirection_10m" INTEGER,
                "mean_7_days_winddirection_10m" INTEGER,
                "std_7_days_winddirection_10m" INTEGER
            );""",
    )

    @task()
    def extract():
        """
        #### Extract task
        We do a GET request, flatten it to get a csv format and dump it onto a disk (data lake in the future).
        The they are pushed into a database. Note that this table is recreated on each run.
        """
        url = f"https://api.open-meteo.com/v1/ecmwf?latitude=48.1482&longitude=17.1067&hourly=temperature_2m,relativehumidity_2m,rain,windspeed_10m,winddirection_10m"

        response = requests.request("GET", url)
        json_data = response.json()

        # flatten the data
        values_raw = list(json_data['hourly'].values())
        values = list(zip(range(1, len(values_raw[0]) + 1), *values_raw))
        labels = ["id", "time", "temperature_2m", "relativehumidity_2m", "rain", "windspeed_10m", "winddirection_10m"]

        # dump into the data lake
        now = pendulum.now().int_timestamp
        data_path = "/opt/airflow/dags/files/ecmwf" + str(now) + ".csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        with open(data_path, 'w') as f:
            write = csv.writer(f)
            write.writerow(labels)
            write.writerows(values)

        # write into database
        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY ecmwf FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task()
    def transform():
        """
        #### Transform task
        A simple Transform task which takes the data collected from the open-meteo api,
        picks the latest data and adds some calculated statistics.
        """
        #data = get_last_7_days()
        data = get_forecast()
        df = pd.DataFrame(data, columns = ['id', 'time', 'temperature_2m', "relativehumidity_2m", "rain","windspeed_10m", "winddirection_10m"])
        newest = df.iloc[-1]
        df = df.drop('time', axis=1)
        variances = df.var()
        means = df.mean()

        query = f"""
            INSERT INTO meteo (id,time,
                               actual_temperature ,mean_7_days_temperature,std_7_days_temperature,
                               actual_humidity,mean_7_days_humidity,std_7_days_humidity,
                               actual_rain,mean_7_days_rain,std_7_days_rain,
                               actual_windspeed_10m,mean_7_days_windspeed_10m,std_7_days_windspeed_10m,
                               actual_winddirection_10m,mean_7_days_winddirection_10m,std_7_days_winddirection_10m
                              ) VALUES (
                               1,'{newest["time"]}',
                               {newest["temperature_2m"]},{means['temperature_2m']},{variances['temperature_2m']},
                               {newest["relativehumidity_2m"]},{means["relativehumidity_2m"]},{variances["relativehumidity_2m"]},
                               {newest["rain"]},{means["rain"]},{variances["rain"]},
                               {newest["windspeed_10m"]},{means["windspeed_10m"]},{variances["windspeed_10m"]},
                               {newest["winddirection_10m"]},{means["winddirection_10m"]},{variances["winddirection_10m"]}
                              );
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    @task()
    def load():
        """
        #### Load task
        We just print the content of the database containing the latest measurement and the calculated (rolling mean and variance) data.
        """
        df = pd.DataFrame(get_meteo_db(), columns = ['id', 'time',
                'actual_temperature','mean_7_days_temperature','std_7_days_temperature',
                'actual_humidity','mean_7_days_humidity','std_7_days_humidity',
                'actual_rain','mean_7_days_rain','std_7_days_rain',
                'actual_windspeed_10m','mean_7_days_windspeed_10m','std_7_days_windspeed_10m',
                'actual_winddirection_10m','mean_7_days_winddirection_10m','std_7_days_winddirection_10m'])
        logging.info(df.to_string())

    [create_ecmwf_table, create_meteo_table] >> extract() >> transform() >> load()

open_meteo_running_avgs()
