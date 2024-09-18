import pathlib
import configparser
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import sys
from psycopg2 import sql
from datetime import datetime
import boto3
import json
import pandas as pd

scripts_path = str(pathlib.Path(__file__).parents[1].resolve() / 'scripts').replace('\\', '/')
sys.path.insert(1, scripts_path)

import connect_to_redshift
import load_to_s3

# redshift and s3 variables
RS_USERNAME = connect_to_redshift.RS_USERNAME
RS_PASSWORD = connect_to_redshift.RS_PASSWORD
RS_HOSTNAME = connect_to_redshift.RS_HOSTNAME
RS_PORT = connect_to_redshift.RS_PORT
RS_ROLE = connect_to_redshift.RS_ROLE
RS_DB = connect_to_redshift.RS_DB
ACCOUNT_ID = connect_to_redshift.ACCOUNT_ID
TABLE_NAME = connect_to_redshift.TABLE_NAME
BUCKET_NAME = load_to_s3.BUCKET_NAME
AWS_REGION = load_to_s3.AWS_REGION

s3_folder_path = f"s3://{BUCKET_NAME}/json/mart-may"
json_filename = "daily-activity.json"

# functions for dag

def _last_processed_date(ti):
    try:
        conn = connect_to_redshift.connect(host=RS_HOSTNAME, user=RS_USERNAME, password=RS_PASSWORD, dbname=RS_DB, port=RS_PORT)
        curr = conn.cursor()

        create_table = connect_to_redshift.create_table_if_doesnt_exist()
        curr.execute(create_table)
        
        last_date = sql.SQL(
            """SELECT dateFor FROM {table} ORDER BY dateFor DESC LIMIT 1;"""
        ).format(table = sql.Identifier(TABLE_NAME))
        curr.execute(last_date)
        
        try:
            fetched_date = curr.fetchone()[0].strftime("%Y-%m-%d")
            curr.close()
            conn.close()
        except Exception as e:
            print(f"Error fetched_date: {e}")
            fetched_date = "2016-03-24" # start date
        finally:
            curr.close()
            conn.close()
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        fetched_date = None
        
    ti.xcom_push(key='fetchedDate', value=fetched_date)
    

def _validate_date(ti):
    fetched_date = ti.xcom_pull(key='fetchedDate', task_ids = 'last_processed_date')
    if fetched_date == None:
        return 'end_run'
    else:
        return 'parse_json'
    
def _parse_json(ti):
    # read and parse json files, save to a csv
    fetched_date_string = ti.xcom_pull(key='fetchedDate', task_ids = 'last_processed_date')
    fetched_date = datetime.strptime(fetched_date_string, "%Y-%m-%d")
    
    s3 = boto3.client('s3')
    
    # daily-activity.json
    s3_file_path = f"{s3_folder_path}/{json_filename}"
    
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_file_path)
        obj_content = obj['Body'].read().decode('utf-8', errors='ignore')
        
        json_data = json.loads(obj_content)
    except Exception as e:
        print(f"Error fetching or parsing the JSON file: {e}")
        return 'end_run'
    
    parsed_json_daily_activity = []
    for activity_dict in json_data:
        if "ActivityDate" in activity_dict.keys():
            # ako je zadnji obradjeni datum stariji od ovog iz recnika
            if fetched_date <= datetime.strptime(activity_dict["ActivityDate"], "%Y-%m-%d"):
                parsed_json_daily_activity.append(activity_dict)
    
    if len(parsed_json_daily_activity) > 0:
        df = pd.DataFrame(parsed_json_daily_activity)
        try:
            df.to_csv('/opt/airflow/spark_files/parsed_data_daily_activity.csv',
                            encoding='utf8',
                            index=False,
                            header=True)
        except Exception as e:
            print(f"Error saving CSV file: {e}")
            return 'end_run'
        return 'spark_process'
    return 'end_run'
    
                
def _save_to_redshift():
    pass

# creating dag
default_args = {
    'owner': 'zigi_stardast',
    'start_date': datetime(2016,3,24), 
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

with DAG('fitness-dag',
         schedule_interval=None,
         default_args=default_args,
         catchup=False) as dag:
    
    last_processed_date = PythonOperator(
        task_id="last_processed_date",
        python_callable=_last_processed_date
    )
    
    validate_date = BranchPythonOperator(
        task_id="validate_date",
        python_callable=_validate_date,
        do_xcom_push=False
    )
    
    parse_json = BranchPythonOperator(
        task_id="parse_json",
        python_callable=_parse_json,
        do_xcom_push=False
    )
    
    spark_process = BashOperator(
        task_id="spark_process",
        bash_command="python /opt/airflow/sparkFiles/spark_process.py"
    )
    
    save_to_redshift = PythonOperator(
        task_id="save_to_redshift",
        python_callable=_save_to_redshift
    )
    
    end_run = EmptyOperator(
        task_id="end_run",
        trigger_rule="none_failed_or_skipped"
    )
    
    last_processed_date >> validate_date
    validate_date >> [parse_json, end_run]
    parse_json >> [spark_process, end_run]
    spark_process >> save_to_redshift >> end_run
    