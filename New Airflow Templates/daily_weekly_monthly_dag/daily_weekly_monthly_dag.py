import datetime
import calendar
import logging
import sys, os
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
 
logging.basicConfig(level=logging.INFO)
sys.path.append(os.path.abspath(os.path.dirname('util')))

##########################
# VARIABLES & PARAMETERS #
##########################
logger = logging.getLogger(__name__)
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
daily_table = 'table_daily'                                                 # Name of daily table
weekly_table = 'table_weekly'                                               # Name of weekly table
monthly_table = 'table_monthly'                                             # Name of monthly table

daily_query = 'sql/daily_query.sql'                                         # Change absolute path IF you change name of sql file
weekly_query = 'sql/weekly_query.sql'                                       # Change absolute path IF you change name of sql file
monthly_query = 'sql/monthly_query.sql'                                     # Change absolute path IF you change name of sql file

####################
# HELPER FUNCTIONS #
####################
def check_week_begin(**kwargs):

    current_date = str(kwargs['ds'])
    
    return datetime.datetime.strptime(current_date, '%Y-%m-%d').date().weekday() == 6
 

def check_month_end(**kwargs):

    current_date = str(kwargs['ds'])
    current_year = datetime.datetime.strptime(current_date, '%Y-%m-%d').year
    current_month = datetime.datetime.strptime(current_date, '%Y-%m-%d').month
    current_day = datetime.datetime.strptime(current_date, '%Y-%m-%d').day

    last_day = calendar.monthrange(year = current_year, month = current_month)[1]

    return current_day == last_day

####################
####### DAG ########
####################
with models.DAG(
        'dataset.table',                                                    # Name DAG <dataset name>.<table_name>
        schedule_interval= '* * * * *',                                     # Specify cron schedule
        template_searchpath = CUR_DIR,
        default_args=default_dag_args,
        max_active_runs = 1,
        catchup=True
) as dag:

    start = DummyOperator(
       task_id='start_run',
       wait_for_downstream = True,
       dag=dag
    )

    check_day_of_week = ShortCircuitOperator(
        task_id = "check_day_of_week",
        python_callable = check_week_begin,
        provide_context = True
    )

    check_day_of_month = ShortCircuitOperator(
        task_id = "check_day_of_month",
        python_callable = check_month_end,
        provide_context = True
    )

    main_run = BigQueryOperator(
        task_id = 'daily_run',
        sql = daily_query,
        bigquery_conn_id = 'bigquery_drive',
        write_disposition = 'WRITE_APPEND',                                 # WRITE_APPEND or WRITE_TRUNCATE
        time_partitioning = {'field':'status_date',                         # Partition by date if possible, otherwise comment out
                            'type':'DAY'},
        destination_dataset_table = '{0}.{1}.{2}'.format(project_id, dataset, daily_table),
        use_legacy_sql=False
    )

    weekly_run = BigQueryOperator(
        task_id ='weekly_run',
        sql = weekly_query,
        bigquery_conn_id = 'bigquery_drive',
        write_disposition ='WRITE_APPEND',                                  # WRITE_APPEND or WRITE_TRUNCATE
        time_partitioning = {'field':'status_date',                         # Partition by date if possible, otherwise comment out
                           'type':'DAY'},
        destination_dataset_table = '{0}.{1}.{2}'.format(project_id, dataset, weekly_table),
        use_legacy_sql = False
    )

    monthly_run = BigQueryOperator(
        task_id ='monthly_run',
        sql = monthly_query,
        bigquery_conn_id = 'bigquery_drive',
        write_disposition ='WRITE_APPEND',                                  # WRITE_APPEND or WRITE_TRUNCATE
        time_partitioning = {'field':'status_date',                         # Partition by date if possible, otherwise comment out
                            'type':'DAY'},
        destination_dataset_table = '{0}.{1}.{2}'.format(project_id, dataset, monthly_table),
        use_legacy_sql = False
    )

    #################
    # ORCHESTRATION #
    #################
    start >> main_run  >> [check_day_of_week, check_day_of_month]
    check_day_of_week >> weekly_run
    check_day_of_month >> monthly_run