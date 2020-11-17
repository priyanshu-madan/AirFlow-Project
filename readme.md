Egen Assesment 


This Repository consists of all the modules necessary to run Airflow on docker

1) Clone the repository
2) Turn on docker compose 
3) Type in the following command in terminal to compose docker image with airflow
    
    docker-compose -f docker-compose-LocalExecutor.yml up -d
    
4) open browser and go to http://localhost:8080/
5) Turn on Egen_Dag

Sample JSON and CSV files generated from this DAG are stored in dags/src/data folder.

The DAG is Scheduled to run daily at 12:00 AM LocalTime


TODO:
1) Debug Postgres connection error (Relation table not found)
2) Create Search Index Database from Postgres tables
3) Create visualizations in jupyter notebook

