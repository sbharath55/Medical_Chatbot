from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from pubmed_script import (
    fetch_pubmed_full,
    download_existing_blob,
    merge_csvs,
    upload_to_blob,
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_pubmed_extraction_and_upload():
    # --- Config (replace with your actual connection string before running) ---
    query = '(("Cardiovascular Diseases"[MeSH] OR "Heart Diseases"[MeSH])) AND (2010:2025[dp])'
    conn_str = "PASTE-YOUR-AZURE-CONNECTION-STRING-HERE"
    container = 'pubmed-data'
    blob_name = 'pubmed_combined.csv'

    # Local working files
    work_dir = "/home/azureuser"
    new_csv    = os.path.join(work_dir, "pubmed_new.csv")
    old_csv    = os.path.join(work_dir, "pubmed_existing.csv")
    merged_csv = os.path.join(work_dir, "pubmed_combined.csv")

    # 1) Fetch new results
    fetch_pubmed_full(query=query, max_results=1000, out_path=new_csv)

    # 2) Download existing dataset (if any)
    existing = download_existing_blob(conn_str, container, blob_name, old_csv)

    # 3) Merge old + new
    merge_csvs(existing, new_csv, merged_csv)

    # 4) Upload merged back
    upload_to_blob(merged_csv, container, blob_name, conn_str)

with DAG(
    dag_id='pubmed_to_csv_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    description='Monthly PubMed → cumulative CSV → Azure Blob',
) as dag:
    PythonOperator(
        task_id='fetch_merge_upload',
        python_callable=run_pubmed_extraction_and_upload,
    )
