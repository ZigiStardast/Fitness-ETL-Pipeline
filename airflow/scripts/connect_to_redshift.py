import psycopg2
import configparser
from psycopg2 import sql
import sys
import pathlib

# configuration file
parser = configparser.ConfigParser()
config_file_name = "configuration.conf"
config_file_path = str(pathlib.Path(__file__).parent.resolve() / config_file_name).replace('\\', '/')
parser.read(config_file_path)

# redshift variables
RS_USERNAME = parser.get("aws_config", "redshift_username")
RS_PASSWORD = parser.get("aws_config", "redshift_password")
RS_HOSTNAME = parser.get("aws_config", "redshift_hostname")
RS_PORT = parser.get("aws_config", "redshift_port")
RS_ROLE = parser.get("aws_config", "redshift_role")
RS_DB = parser.get("aws_config", "redshift_database")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "fitness"

def create_table_if_doesnt_exist():
    sql_create_table = sql.SQL(
        """ CREATE TABLE IF NOT EXISTS {table} (
                                id varchar PRIMARY KEY,
                                dateFor date
            );
        """
    ).format(table=sql.Identifier(TABLE_NAME))
    
    return sql_create_table

def connect(host, user, password, dbname, port):
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, dbname=dbname, port=port)
        return conn
    except Exception as e:
        print(f"Unable to connect to redshift: {e}")
        sys.exit(1)
