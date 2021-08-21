### SETUP ###
import datetime
import logging
import sys, os

from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

sys.path.append(os.path.abspath(os.path.dirname('util')))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


### VARIABLES ###
dag_name = "name"
start_date = datetime.datetime(2021, 8, 19)
cron_interval = '30 12 * * *'
bq_destination_table = 'tensile-oarlock-191715.afa_propensity.afa_propensity_evaluated_score'
query = ''' '''

### DAG ###
default_dag_args = {
    'depends_on_past': True,
    'start_date': start_date,
    'email': ['w7b3x9v1n4u6k3j6@kohoteam.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1)
}

with models.DAG(
        dag_name,
        schedule_interval= cron_interval,
        default_args=default_dag_args,
        catchup=True
) as dag:

    bigquery_table = BigQueryOperator(
        task_id='CHANGE',
        sql = query,
        bigquery_conn_id = 'bigquery_drive',
        write_disposition ='WRITE_APPEND', # WRITE_TRUNCATE OR WRITE APPEND
        destination_dataset_table = bq_destination_table,
        use_legacy_sql=False)

    bigquery_table