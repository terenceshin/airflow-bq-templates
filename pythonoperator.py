### SETUP ###
import datetime
import pandas_gbq as pgbq

from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator

### VARIABLES ###
dag_name = "name"
start_date = datetime.datetime(2021, 8, 19)
cron_interval = '30 12 * * *'
bq_destination_table = 'tensile-oarlock-191715.afa_propensity.afa_propensity_evaluated_score'

### DAG ###
default_dag_args = {
    'depends_on_past': False,
    'start_date': start_date,
    'email': ['w7b3x9v1n4u6k3j6@kohoteam.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(dag_id = dag_name,
          default_args = default_dag_args,
          schedule_interval = cron_interval,
          catchup = False)


def create_function():

    query = """ """
    df = pgbq.read_gbq(query, project_id = 'tensile-oarlock-191715', dialect = 'standard')
    pgbq.to_gbq(df, 'premium_models.user_data', project_id = 'tensile-oarlock-191715', if_exists='replace')


with dag:

    python_function = PythonOperator(
        task_id='task',
        python_callable= create_function
    )

    python_function