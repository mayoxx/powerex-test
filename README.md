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


## Example plot for temperature and rolling averages to verify the data

    import matplotlib.pyplot as plt
    #import matplotlib.dates as dt
    import pandas as pd
    import requests

    # LOAD FROM meteo DB, here loaded by GET request
    url = f"https://api.open-meteo.com/v1/ecmwf?latitude=48.1482&longitude=17.1067&hourly=temperature_2m,relativehumidity_2m,rain,windspeed_10m,winddirection_10m"
    response = requests.request("GET", url)
    json_data = response.json()
    values_raw = list(json_data['hourly'].values())
    data = list(zip(range(len(values_raw[0])), *values_raw))
    labels = ["id", "time", "temperature_2m", "relativehumidity_2m", "rain", "windspeed_10m", "winddirection_10m"]
    df = pd.DataFrame(data, columns=[labels])
    df['mean_rolling_temperature_2m'] = round(df['temperature_2m'].rolling(4, min_periods=1).mean(), 2)
    # LOAD FROM meteo DB

    temperature = df['temperature_2m']
    t_average = df['mean_rolling_temperature_2m']
    # timedates = dt.date2num(df['time'])

    plt.figure(figsize=(10, 5))
    plt.plot(temperature, 'k-', label='Original')
    plt.plot(t_average, 'r-', label='Running average')
    # plt.plot_date(timedates, temperature)
    plt.ylabel('Temperature (deg C)')
    plt.xlabel('Date')
    plt.grid(linestyle=':')
    plt.fill_between(t_average.index, 0, t_average.squeeze(), color='r', alpha=0.1)
    plt.legend(loc='upper left')
    plt.show()