# Lw
step 1 run
$ git clone https://github.com/jaroensakyod/Lw.git


step 2
$ mkdir -p ./dags ./logs ./plugins ./data


step 3
$ docker build -t apache/airflow:2.1.0 .


step 4
$ docker-compose up airflow-init


step 5
$ docker-compose up


step 6
look at user:  airflow //password:  airflow


http://localhost:8080/


step 7
run
DAG Pipeline
