from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryInsertJobOperator, BigQueryUpdateTableSchemaOperator
from airflow import DAG
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH
from sql.sensor.vendas_marca_linha import SENSOR_QUERIES

table_id = "vendas_marca_linha"
dataset_id = "refined"
sql = f"/sql/load/{table_id}.sql"
schema = f"/sql/schema/{table_id}.json"

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
    f"gb_insert_data_{table_id}",
    start_date=datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "insert", f"{dataset_id}"],
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
                "task_id": f"insert_query_job_{table_id}"[:60].lower(),
                "dag_id": dag.dag_id[:60].lower()
            }
        }
    )

    update_table_schema = BigQueryUpdateTableSchemaOperator(
        task_id=f"update_table_schema_{table_id}",
        dataset_id=f"{dataset_id}",
        table_id=f"{table_id}",
        schema_fields_updates= f"{{% include '{schema}' %}}"
    )


sensor_task >> insert_query_job >> update_table_schema