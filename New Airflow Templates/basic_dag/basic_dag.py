import datetime
import logging
import sys, os

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

sys.path.append(os.path.abspath(os.path.dirname('util')))

default_dag_args = {
    'depends_on_past': True,
    'start_date': datetime.datetime(9999, 12, 31),                           # Specify start date
    'email': ['w7b3x9v1n4u6k3j6@kohoteam.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1)
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

query = "sql/query.sql"                                                     # Change absolute path IF you change name of sql file

with models.DAG(
        'dataset.table',                                                    # Name DAG <dataset name>.<table_name>
        schedule_interval= '* * * * *',                                     # Specify cron schedule
        default_args=default_dag_args,
        catchup=True
) as dag:

    main_query = BigQueryOperator(
        task_id='main_query',                                               # Optional: Name task 
        sql = query,
        bigquery_conn_id = 'bigquery_drive',
        write_disposition= 'WRITE_APPEND',                                  # WRITE_APPEND or WRITE_TRUNCATE
        time_partitioning= {'field':'status_date',                          # Partition by date if possible, otherwise comment out
                            'type':'DAY'},
        destination_dataset_table='project_id.dataset.table',               # Specify destination table
        use_legacy_sql=False)

    main_query
                                 