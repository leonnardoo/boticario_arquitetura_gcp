from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH, SA_PATH
from google.cloud import storage
import pandas as pd


default_args = {
    "owner": "Engenharia de Dados",
    "depends_on_past": False,
    "email": ["leonnardo_rj@hotmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1
}

#Rodando todo dia as 6h em UTC-3
schedule_interval = "0 6 * * *"

with DAG(
    "gb_insert_data",
    start_date=datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "Insert"],
) as dag:
    
    @task(task_id="excel_to_csv")
    def excel_to_csv(**kwargs):
        client = storage.Client()
        blobs = client.list_blobs("raw_data_boticario")

        for blob in blobs:
            src_file = client.bucket("raw_data_boticario").blob(blob.name).download_as_bytes()
            dst_file = client.bucket("refined_data_boticario").blob(blob.name.replace(".xlsx",".csv"))

            df = pd.read_excel(src_file, index_col=0).to_csv()

            dst_file.upload_from_string(df)

        

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="insert_data_task",
        bucket="refined_data_boticario",
        source_objects=["Base_*.csv"],
        destination_project_dataset_table="refined.base_anos",
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

excel_to_csv() >> gcs_to_bq