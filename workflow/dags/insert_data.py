from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow import DAG
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH

schema = "sql/schema/base_anos.json"


default_args = {
    "owner": "Engenharia de Dados",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
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
    "insert_data",
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "Insert"],
) as dag:
    
    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="insert_data_task",
        bucket="raw_data_boticario",
        source_objects=["Base_*.xlsx"],
        destination_project_dataset_table="refined.base_anos",
        schema_fields=f"{{% include '{schema}' %}}",
        write_disposition='WRITE_TRUNCATE',
    )

    #insert_query_job = BigQueryInsertJobOperator(
    #    task_id="insert_query_job",
    #    configuration={
    #        "query": {
    #            "query": INSERT_ROWS_QUERY,
    #            "useLegacySql": False,
    #        }
    #    },
    #    location=location,
    #)

gcs_to_bq