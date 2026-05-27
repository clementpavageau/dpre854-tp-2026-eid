from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
import pandas as pd
import requests

LOG_FILE = "/opt/airflow/dags/intrusion.log"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

# -----------------------------------
# Extraction des IPs
# -----------------------------------

def extract_ips(**context):

    ips = []

    with open(LOG_FILE, "r") as f:

        for line in f:

            if "SRC=" in line:

                ip = line.split("SRC=")[1].split(" ")[0]

                ips.append(ip)

    context["ti"].xcom_push(
        key="ips",
        value=ips
    )

# -----------------------------------
# Correspondance IP / Pays
# -----------------------------------

def ip_to_country(**context):

    ips = context["ti"].xcom_pull(
        task_ids="extract_ips",
        key="ips"
    )

    results = []

    for ip in ips:

        response = requests.get(
            f"http://ip-api.com/json/{ip}"
        )

        data = response.json()

        results.append({
            "ip": ip,
            "country": data.get("country", "UNKNOWN")
        })

    context["ti"].xcom_push(
        key="results",
        value=results
    )

# -----------------------------------
# Sauvegarde CSV
# -----------------------------------

def save_csv(**context):

    results = context["ti"].xcom_pull(
        task_ids="ip_to_country",
        key="results"
    )

    df = pd.DataFrame(results)

    df.to_csv(
        "/opt/airflow/dags/result_intrusion.csv",
        index=False
    )

# -----------------------------------
# DAG
# -----------------------------------

with DAG(
    dag_id="dag_intrusion",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_intrusion",
        sql="""
        CREATE TABLE IF NOT EXISTS intrusion_logs (
            id SERIAL PRIMARY KEY,
            ip VARCHAR(50),
            country VARCHAR(100)
        );
        """
    )

    extract_ips_task = PythonOperator(
        task_id="extract_ips",
        python_callable=extract_ips
    )

    ip_to_country_task = PythonOperator(
        task_id="ip_to_country",
        python_callable=ip_to_country
    )

    save_csv_task = PythonOperator(
        task_id="save_csv",
        python_callable=save_csv
    )

    create_table >> extract_ips_task >> ip_to_country_task >> save_csv_task