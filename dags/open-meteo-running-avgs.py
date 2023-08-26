import requests
import pandas as pd
import numpy as np
import logging
import json
import pendulum
import os
import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


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
        postgres_conn_id=DB_ID,
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
        postgres_conn_id=DB_ID,
        sql="""
            DROP TABLE IF EXISTS meteo;
            CREATE TABLE meteo (
                "id" NUMERIC PRIMARY KEY,
                "time" TIMESTAMP,

                "temperature_2m" DOUBLE PRECISION,
                "mean_rolling_temperature_2m" DOUBLE PRECISION,
                "mean_moving_temperature_2m" DOUBLE PRECISION,
                "std_rolling_temperature_2m" DOUBLE PRECISION,
                "std_moving_temperature_2m" DOUBLE PRECISION,

                "relativehumidity_2m" INTEGER,
                "mean_rolling_relativehumidity_2m" INTEGER,
                "mean_moving_relativehumidity_2m" INTEGER,
                "std_rolling_relativehumidity_2m" INTEGER,
                "std_moving_relativehumidity_2m" INTEGER,

                "rain" DOUBLE PRECISION,
                "mean_rolling_rain" DOUBLE PRECISION,
                "mean_moving_rain" DOUBLE PRECISION,
                "std_rolling_rain" DOUBLE PRECISION,
                "std_moving_rain" DOUBLE PRECISION,

                "windspeed_10m" DOUBLE PRECISION,
                "mean_rolling_windspeed_10m" DOUBLE PRECISION,
                "mean_moving_windspeed_10m" DOUBLE PRECISION,
                "std_rolling_windspeed_10m" DOUBLE PRECISION,
                "std_moving_windspeed_10m" DOUBLE PRECISION,

                "winddirection_10m" INTEGER,
                "mean_rolling_winddirection_10m" INTEGER,
                "mean_moving_winddirection_10m" INTEGER,
                "std_rolling_winddirection_10m" INTEGER,
                "std_moving_winddirection_10m" INTEGER
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
        values = list(zip(range(len(values_raw[0])), *values_raw))
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
        postgres_hook = PostgresHook(postgres_conn_id=DB_ID)
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
        data = get_forecast()
        values = ["temperature_2m", "relativehumidity_2m",
                  "rain", "windspeed_10m", "winddirection_10m"]

        df = pd.DataFrame(data, columns=['id', 'time', *values])
        df = df.drop('id', axis=1)

        n = 4
        for val in values:
            df['mean_rolling_' + val] = round(df[val].rolling(
                n, min_periods=1).mean(), 2)
            df['mean_moving_' + val] = round(df[val].rolling(
                df.shape[0], min_periods=1).mean(), 2)
            df['std_rolling_' + val] = round(df[val].rolling(
                n, min_periods=1).std(), 2)
            df['std_moving_' + val] = round(df[val].rolling(
                df.shape[0], min_periods=1).std(), 2)
            
        postgres_hook = PostgresHook(postgres_conn_id=DB_ID)
        df.to_sql('meteo', postgres_hook.get_sqlalchemy_engine(),
                  if_exists='replace', chunksize=1000)

    @task()
    def load():
        """
        #### Load task
        We just print the content of the database containing the latest measurement and the calculated (rolling mean and variance) data.
        """
        column_labels = ['id', 'time', 'temperature_2m', 'relativehumidity_2m', 'rain',
                         'windspeed_10m', 'winddirection_10m', 'mean_rolling_temperature_2m',
                         'mean_moving_temperature_2m', 'std_rolling_temperature_2m',
                         'std_moving_temperature_2m', 'mean_rolling_relativehumidity_2m',
                         'mean_moving_relativehumidity_2m', 'std_rolling_relativehumidity_2m',
                         'std_moving_relativehumidity_2m', 'mean_rolling_rain',
                         'mean_moving_rain', 'std_rolling_rain', 'std_moving_rain',
                         'mean_rolling_windspeed_10m', 'mean_moving_windspeed_10m',
                         'std_rolling_windspeed_10m', 'std_moving_windspeed_10m',
                         'mean_rolling_winddirection_10m', 'mean_moving_winddirection_10m',
                         'std_rolling_winddirection_10m', 'std_moving_winddirection_10m']
        df = pd.DataFrame(get_meteo_db(), columns=column_labels)
        df = df.drop('id', axis=1)
        logging.info(df.to_string())

    [create_ecmwf_table, create_meteo_table] >> extract() >> transform() >> load()

open_meteo_running_avgs()