import pendulum
from airflow.models import Variable

SAO_PAULO_TZ = pendulum.timezone("America/Sao_Paulo")
ROOT_PATH = Variable.get("ROOT_PATH", default_var="/home/airflow/gcs/dags/")
SPOTIFY_CLIENT_ID_API = Variable.get("SPOTIFY_CLIENT_ID_API", default_var="")
SPOTIFY_CLIENT_SECRET_API = Variable.get("SPOTIFY_CLIENT_SECRET_API", default_var="")