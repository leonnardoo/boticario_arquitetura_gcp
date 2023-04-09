from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH
from google.cloud import storage
import pandas as pd
import ndjson


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

nome = "spotify_episodes"

with DAG(
    "gb_insert_api_data_podcast_episodes",
    start_date=datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "insert", "trusted", "refined", "api"],
) as dag:
    
    @task(task_id="json_to_trusted_json", default_args=default_args)
    def json_to_trusted_json(**kwargs):
        client = storage.Client()
        blobs = client.list_blobs("raw_data_boticario")

        datahora_carga = datetime.now(tz=SAO_PAULO_TZ).strftime("%Y-%m-%d %H:%M:%S")

        for blob in blobs:
            if f"api/{nome}" in blob.name:
                src_file = client.bucket("raw_data_boticario").blob(blob.name).download_as_string()
                dst_file = client.bucket("trusted_data_boticario").blob(blob.name)

                src_file = ndjson.loads(src_file)
                json_src_file = src_file[0]['episodes']['items']

                json_list = []

                for item in range(len(json_src_file)):
                    json_list.append({
                        'id': json_src_file[item]['id'],
                        'name': json_src_file[item]['name'],
                        'description': json_src_file[item]['description'],
                        'release_date': json_src_file[item]['release_date'],
                        'duration_ms': json_src_file[item]['duration_ms'],
                        'languages': json_src_file[item]['languages'],
                        'explicit': json_src_file[item]['explicit'],
                        'type': json_src_file[item]['type'],
                        'datahora_carga': datahora_carga
                    })
        
                json_file = pd.DataFrame.from_dict(json_list)
                dst_file.upload_from_string(json_file.to_csv(sep=",", index=False))


    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="insert_api_data_podcast_episodes_task",
        bucket="trusted_data_boticario",
        source_objects=[f"api/{nome}*.json"],
        destination_project_dataset_table="refined_api.spotify_podcast_episodes",
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )



json_to_trusted_json() >> gcs_to_bq