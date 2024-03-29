import datetime
import logging
import sys, os
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
sys.path.append(os.path.abspath(os.path.dirname('util')))

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
 

default_dag_args = {
    'depends_on_past': True,
    'start_date': datetime.datetime(9999, 12, 31),                          # Specify start date
    'email': ['w7b3x9v1n4u6k3j6@kohoteam.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1)
}

project_id = 'tensile-oarlock-191715'                                       # Name of project id for table you're creating
dataset = 'dataset'                                                         # Name of dataset for table you're creating
table = 'table'                                                             # Name of table you're creating
backfill_query = "sql/backfill_query.sql"                                   # Change absolute path IF you change name of sql file
query = "sql/daily_query.sql"                                               # Change absolute path IF you change name of sql file
 
 
with models.DAG(
        'dataset.table',                                                    # Name DAG <dataset name>.<table_name>
        schedule_interval= '* * * * *',                                     # Specify cron schedule
        template_searchpath = CUR_DIR,
        default_args=default_dag_args,
        catchup=True
) as dag:
 
   start = DummyOperator(
       task_id='start_run',
       wait_for_downstream = True,
       dag=dag
   )

   check_operator = BranchSQLOperator(
       task_id = "check_table",
       conn_id = "bigquery_drive",
       sql = "SELECT COUNT(*) FROM {0}.__TABLES_SUMMARY__ WHERE table_id = '{0}'".format(dataset, table),
       follow_task_ids_if_true = "main_run",
       follow_task_ids_if_false = "backfill_query",
       dag = dag
   )
 
   backfill = BigQueryOperator(
       task_id = 'backfill_query',
       sql = backfill_query,
       bigquery_conn_id = 'bigquery_drive',
       write_disposition ='WRITE_APPEND',                                   # WRITE_APPEND or WRITE_TRUNCATE
       time_partitioning = {'field':'status_date',                          # Partition by date if possible, otherwise comment out
                           'type':'DAY'},
       destination_dataset_table = '{0}.{1}.{2}'.format(project_id, dataset, table),
       use_legacy_sql = False
   )
 
   main_run = BigQueryOperator(
       task_id ='main_run',
       sql = query,
       bigquery_conn_id = 'bigquery_drive',
       write_disposition ='WRITE_APPEND',                                   # WRITE_APPEND or WRITE_TRUNCATE
       time_partitioning = {'field':'status_date',                          # Partition by date if possible, otherwise comment out
                           'type':'DAY'},
       destination_dataset_table = '{0}.{1}.{2}'.format(project_id, dataset, table),
       use_legacy_sql = False
   )
 

 
   start >> check_operator >> [backfill, main_run]
   

