# powerex-test

## zadanie

1. Kazdych 6h (ked nastane update) stiahni data o pocasi do tabulky pre aktualny cas(vyber databazy je na tebe). Tieto data budu reprezentovat surove historicke data, ktore budu verziovane podla updatu z ecmwf.
mozna struktura tabulky, e.g. Created, Time, Temperature, Relative_Humidity, ...
  https://api.open-meteo.com/v1/ecmwf?latitude=48.1482&longitude=17.1067&hourly=temperature_2m,relativehumidity_2m,rain,windspeed_10m,winddirection_10m
2. Z predchadzajucich dat priprav dalsiu tabulku, ktora bude pozostavat len s najnovsich dat pre danu hodinu a zaroven priprav dalsie features z povodnych dat ako 7dnovy klzavy priemer a 7dnovu klzavu varianciu.
e.g.:
Time, Actual_Temperature, Mean_7_days_Temperature, Std_7_days_Temperature, Actual_Humidity, Mean_7_days_Humidity, Std_7_days_Humidity, ...

3. Nascheduluj pripravu tychto dvoch tabuliek na kazdych 6 hodin tak aby obsahovali data po update z ecmwf.

Datovu pipelinu zrealizuj v prostredi AWS MWAA.