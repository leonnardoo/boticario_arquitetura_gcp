from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow import DAG
from airflow.decorators import task, task_group
from datetime import datetime, timedelta
from config.utils import SAO_PAULO_TZ, ROOT_PATH, SPOTIFY_CLIENT_ID_API, SPOTIFY_CLIENT_SECRET_API
from google.cloud import storage

import requests
import base64
import json


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

#Rodando todo dia as 5h em UTC-3
schedule_interval = "0 5 * * *"

with DAG(
    "gb_insert_raw_api_data",
    start_date=datetime(2022, 4, 7, tzinfo=SAO_PAULO_TZ),
    catchup=False,
    schedule_interval=schedule_interval,
    default_args=default_args,
    template_searchpath=ROOT_PATH,
    dagrun_timeout=timedelta(minutes=45),
    tags=["Leonnardo Pereira", "insert", "raw", "api"],
) as dag:
    
    def api_to_storage():
        def get_auth_header(token):
            return {'Authorization': f'Bearer {token}'}

        @task(task_id="get_token", default_args=default_args)
        def get_token(client_id, client_secret):
            auth_string = client_id + ":" + client_secret
            auth_bytes = auth_string.encode("utf-8")
            auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
            url = 'https://accounts.spotify.com/api/token'
            headers = {
                'Authorization': f'Basic {auth_base64}',
                'Content-type': 'application/x-www-form-urlencoded'
                }
            data = {'grant_type': 'client_credentials'}
            response = requests.post(url, headers=headers, data=data)
            json_result = json.loads(response.content)
            token = json_result["access_token"]

            return token

        @task(task_id="search_podcast", default_args=default_args)
        def search_podcast(token, term, type):
            url = 'https://api.spotify.com/v1/search'
            headers = get_auth_header(token)
            query_string = f"?q={term}&type={type}&limit=50&market=BR"
            response = requests.get(url+query_string, headers=headers)
            json_result = json.loads(response.content)
            if len(json_result['shows']['items']) == 0:
                pass
            else:
                return json_result

        @task(task_id="insert_json_to_storage", default_args=default_args)
        def insert_json_to_storage(json_results, term: str, type, bucket):
            if json_results:
                data_carga = datetime.now(tz=SAO_PAULO_TZ).strftime("%Y%m%d")
                client_bucket = storage.Client().get_bucket(bucket)
                term = term.replace("+","_")
                data = json.dumps(json_results)
                blob = client_bucket.blob(f"api/spotify_{type}_{term}_{data_carga}.json")
                blob.upload_from_string(data=data)
            else:
                pass

            return

        @task(task_id="search_podcast_episodes", default_args=default_args)
        def search_podcast_episodes(token, id):
            url = f"https://api.spotify.com/v1/shows/{id}?market=BR&limit=100"
            headers = get_auth_header(token)
            response = requests.get(url, headers=headers)
            json_result = json.loads(response.content)
            if len(json_result) == 0:
                pass
            else:
                return json_result

        @task(task_id="insert_json_episodes_to_storage", default_args=default_args)
        def insert_json_episodes_to_storage(json_results, term, bucket):
            if json_results:
                data_carga = datetime.now(tz=SAO_PAULO_TZ).strftime("%Y%m%d")
                client_bucket = storage.Client().get_bucket(bucket)
                term = term.replace("+","_")
                data = json.dumps(json_results)
                blob = client_bucket.blob(f"api/spotify_episodes_{term}_{data_carga}.json")
                blob.upload_from_string(data=data)
            else:
                pass
            
            return

        client_id = SPOTIFY_CLIENT_ID_API
        client_secret = SPOTIFY_CLIENT_SECRET_API
        term = "data+hackers"
        bucket = "raw_data_boticario"
        type = "show"
        id = "1oMIHOXsrLFENAeM743g93"
        token = get_token(client_id, client_secret)

        insert_json_to_storage(search_podcast(token, term, type), term, type, bucket)
        insert_json_episodes_to_storage(search_podcast_episodes(token, id), term, bucket)

    api_to_storage()