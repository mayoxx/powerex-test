FROM apache/airflow:2.7.0

ADD requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
