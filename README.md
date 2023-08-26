# powerex-test

## Connection

- Connection Id: tutorial_pg_conn
- Connection Type: postgres
- Host: postgres
- Schema: airflow
- Login: airflow
- Password: airflow
- Port: 5432

## Run in docker

    echo -e "AIRFLOW_UID=$(id -u)" > .env
    AIRFLOW_UID=50000
    docker compose up airflow-init
    docker compose build
    docker compose up
