from datetime import timedelta, date, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import PostgresOperator
from airflow.hooks import PostgresHook
import requests
import json
import os
import pandas as pd
from pandas.io.json import json_normalize
import glob
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    'schedule_interval': '@daily'
}

dag = DAG("egen_dag", default_args=default_args)

url_lists = {
    "california": "https://data.chhs.ca.gov/data.json",
    "maryland": "https://opendata.maryland.gov/data.json",
    "colorado": "https://data.colorado.gov/data.json",
    "michigan": "https://data.michigan.gov/data.json",
    "missouri": "https://data.mo.gov/data.json",
    "newyork": "https://health.data.ny.gov/data.json",
    "oklahoma": "https://data.ok.gov/data.json",
    "oregon": "https://data.oregon.gov/data.json",
    "washington": "https://data.wa.gov/data.json",
    "chicago": "https://data.cityofchicago.org/data.json"
}



def delete_files():

    """
    This task removes all the existing Json files at the begining of the DAG
    :return:
    """

    file_lists = glob.glob("/usr/local/airflow/dags/src/data/*.json")
    print(file_lists)

    for files in file_lists:
        try:
            os.remove(files)
        except:
            print("file not found", files)


def download_json(country, country_url):
    """
    This task uses Request module to access the API urls of each state and saves the json file to local storage

    :param country:
    :param country_url:
    :return:
    """
    res = requests.get(url=country_url)
    print(res.status_code)

    if res.status_code == 200:

        json_data = res.json()["dataset"]
        file_name = country + '.json'
        tot_name = '/usr/local/airflow/dags/src/data/' + file_name
        print("uploading" + tot_name)

        with open(tot_name, 'w+') as outputfile:
            json.dump(json_data, outputfile)

    else:
        print("Error In API call.")


def read_csv(country):
    """
    This task reads the saved JSON files present in the storage, Parses relevant information and stores
    it as a dataframe and a CSV file for each state.
    :param country:
    :return:
    """
    engine = create_engine("postgresql://airflow:airflow@postgres")
    conn = engine.connect().connection
    cursor = conn.cursor()


    with open("/usr/local/airflow/dags/src/data/" + country + ".json") as data_file:
        data = json.load(data_file)

    df = json_normalize(data)
    df["search"] = df["title"] + df['description']
    df = df[df["search"].str.contains("covid|covid19|covid-19|coronavirus", case=False)]
    df['keyword'] = df['keyword'].apply(lambda d: d if isinstance(d, list) else [])
    df['title'] = df['title'].apply(lambda d: d if isinstance(d, str) else "NA")
    df['description'] = df['description'].apply(lambda d: d if isinstance(d, str) else "NA")
    df['distribution'] = df['distribution'].apply(lambda d: d if isinstance(d, list) else [])
    df['theme'] = df['theme'].apply(lambda d: d if isinstance(d, list) else [])
    json2 = df.to_dict(orient="records")

    result_df = json_normalize(data=json2, record_path=['distribution'],meta=["title", "description", "modified", "accessLevel", "identifier", "landingPage"], record_prefix='distribution_')
    result_df.columns = result_df.columns.str.replace('[#,@,&,.]', '')

    result_df = result_df.astype(str)



    result_df.to_csv("/usr/local/airflow/dags/src/data/" + country + ".csv", index=False)

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in result_df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(result_df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,)" % (country, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()
    print("DATABASE SAVED TO CSV")






delete_files = PythonOperator(
    task_id='deleting_files',
    python_callable=delete_files,
    dag=dag
)

b = []

for each in url_lists.items():
    table_name = each[0]
    b.append(

        PythonOperator(
            task_id='readingcsv' + each[0],
            python_callable=read_csv,
            op_kwargs={'country': each[0]},
            dag=dag
        )

        <<

        PythonOperator(
            task_id='downloading' + each[0],
            python_callable=download_json,
            op_kwargs={'country': each[0], 'country_url': each[1]},
            dag=dag
        )
    )

delete_files >> b