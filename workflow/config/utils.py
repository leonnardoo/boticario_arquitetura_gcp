import pendulum
from airflow.models import Variable

SAO_PAULO_TZ = pendulum.timezone("America/Sao_Paulo")
ROOT_PATH = Variable.get("ROOT_PATH", default_var="/home/airflow/gcs/dags/")