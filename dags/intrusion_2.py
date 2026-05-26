from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from datetime import datetime
import pandas as pd
import requests

# -----------------------------------
# Dataset partagé
# -----------------------------------

dataset_ips = Dataset("/opt/airflow/data/ip_inventory.csv")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

# -----------------------------------
# DAG 1
# -----------------------------------

def extract_log_ips():

    ips = []

    with open("/opt/airflow/data/intrusion.log") as f:

        for line in f:
            if "SRC=" in line:
                ip = line.split("SRC=")[1].split(" ")[0]
                ips.append(ip)

    df = pd.DataFrame({"ip": list(set(ips))})

    df.to_csv(
        "/opt/airflow/data/ip_inventory.csv",
        index=False
    )

with DAG(
    dag_id="dag_intrusion_log",
    schedule="@hourly",
    catchup=False,
    default_args=default_args
) as dag_intrusion_log:

    extract_ips = PythonOperator(
        task_id="extract_ips",
        python_callable=extract_log_ips,
        outlets=[dataset_ips]
    )

# -----------------------------------
# DAG 2
# -----------------------------------

def enrich_country():

    df = pd.read_csv(
        "/opt/airflow/data/ip_inventory.csv"
    )

    results = []

    for ip in df["ip"]:

        response = requests.get(
            f"http://ip-api.com/json/{ip}"
        )

        data = response.json()

        results.append({
            "ip": ip,
            "country": data.get("country")
        })

    result_df = pd.DataFrame(results)

    result_df.to_csv(
        "/opt/airflow/data/ip_country.csv",
        index=False
    )

with DAG(
    dag_id="dag_intrusion_pays",
    schedule=[dataset_ips],
    catchup=False,
    default_args=default_args
) as dag_intrusion_pays:

    enrich_task = PythonOperator(
        task_id="enrich_country",
        python_callable=enrich_country
    )

# -----------------------------------
# DAG 3
# -----------------------------------

def build_final_dataset():

    ips = pd.read_csv(
        "/opt/airflow/data/ip_inventory.csv"
    )

    countries = pd.read_csv(
        "/opt/airflow/data/ip_country.csv"
    )

    final_df = ips.merge(
        countries,
        on="ip",
        how="left"
    )

    final_df.to_csv(
        "/opt/airflow/data/final_intrusion.csv",
        index=False
    )

with DAG(
    dag_id="dag_intrusion_db",
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag_intrusion_db:

    build_dataset = PythonOperator(
        task_id="build_final_dataset",
        python_callable=build_final_dataset
    )