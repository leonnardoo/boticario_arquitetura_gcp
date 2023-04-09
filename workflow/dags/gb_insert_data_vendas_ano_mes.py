from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow import DAG
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH

nome = "vendas_ano_mes"
sql = f"/sql/load/{nome}.sql"

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
    f"gb_insert_data_{nome}",
    start_date=datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "Insert", "Refined"],
) as dag:

    insert_query_job = BigQueryInsertJobOperator(
    task_id= f"insert_query_job_{nome}",
    configuration={
        "jobType": "QUERY",
        "query": {
            "query": f"{{% include '{sql}' %}}",
            "destinationTable": {
                    "projectId": "singular-arcana-383119",
                    "datasetId": "refined",
                    "tableId": f"{nome}"
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            "useLegacySql": False,
        },
        "labels":{
            "task_id": f"insert_query_job_{nome}"[:60].lower(),
            "dag_id": dag.dag_id[:60].lower()
            }
        }
    )

