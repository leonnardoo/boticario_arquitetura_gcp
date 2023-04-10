from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryInsertJobOperator
from airflow import DAG
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH
from sql.sensor.spotify_podcast_episodes_gb import SENSOR_QUERIES

table_id = "spotify_podcast_episodes_gb"
dataset_id = "refined_api"
sql = f"/sql/load/{table_id}.sql"

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

#Rodando todo dia as 6:30h em UTC-3
schedule_interval = "30 6 * * *"

with DAG(
    f"gb_insert_api_data_{table_id}",
    start_date=datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "processing", f"{dataset_id}"],
) as dag:

    sensor_task =  BigQueryValueCheckOperator(
        task_id=f"sensor_task_{table_id}",
        sql= SENSOR_QUERIES[table_id],
        pass_value=True,
        use_legacy_sql=False,
        retries=12,
        retry_delay=300,
    ) 

    insert_query_job = BigQueryInsertJobOperator(
        task_id= f"insert_query_job_{table_id}",
        configuration={
            "jobType": "QUERY",
            "query": {
                "query": f"{{% include '{sql}' %}}",
                "destinationTable": {
                        "projectId": "singular-arcana-383119",
                        "datasetId": f"{dataset_id}",
                        "tableId": f"{table_id}"
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            },
            "labels":{
                "task_id": f"{table_id}"[:60].lower(),
                "dag_id": dag.dag_id[:60].lower()
            }
        }
    )


sensor_task >> insert_query_job