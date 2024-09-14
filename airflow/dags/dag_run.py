import pathlib
import configparser
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# configuration file
parser = configparser.ConfigParser()
config_file_name = "configuration.conf"
config_file_path = str(pathlib.Path(__file__).parents[1].resolve() / f'scripts/{config_file_name}').replace('\\', '/')

# redshift variables
RS_USERNAME = parser.get("aws_config", "redshift_username")
RS_PASSWORD = parser.get("aws_config", "redshift_password")
RS_HOSTNAME = parser.get("aws_config", "redshift_hostname")
RS_PORT = parser.get("aws_config", "redshift_port")
RS_ROLE = parser.get("aws_config", "redshift_role")
RS_DB = parser.get("aws_config", "redshift_database")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "fitness"

# functions

def _last_processed_date():
    pass

def _validate_date():
    pass

def _parse_json():
    pass

def _save_to_redshift():
    pass


# creating dag
default_args = {
    'owner': 'zigi_stardast',
    'start_date': datetime(2024,10,1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

with DAG('fitness-dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    
    last_processed_date = PythonOperator(
        task_id="last_processed_date",
        python_callable=_last_processed_date
    )
    
    validate_date = BranchPythonOperator(
        task_id="validate_date",
        python_callable=_validate_date
    )
    
    parse_json = BranchPythonOperator(
        task_id="parse_json",
        python_callable=_parse_json
    )
    
    spark_process = BashOperator(
        task_id="spark_process",
        bash_command=""
    )
    
    save_to_redshift = PythonOperator(
        task_id="save_to_redshift",
        python_callable=_save_to_redshift
    )