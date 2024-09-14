import boto3
import botocore
import configparser
import pathlib
import sys
import os
import botocore.exceptions

# configuration files
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file_name = "configuration.conf"
config_file_path = str(script_path / config_file_name).replace('\\', '/')
parser.read(config_file_path)

# variables
if parser.has_section("aws_config"):
    BUCKET_NAME = parser.get("aws_config", "bucket_name")
    AWS_REGION = parser.get("aws_config", "aws_region")
else:
    raise Exception("Configuration section 'aws_config' not found in configuration.conf")

def main():
    s3_conn = connect_to_s3()
    create_bucket_if_not_exists(s3_conn)
    upload_to_s3(s3_conn)
    
def connect_to_s3():
    try:
        conn = boto3.resource('s3')
        return conn
    except Exception as e:
        print(f"Can't connect to S3. Error: {e}")
        sys.exit(1)
        
def create_bucket_if_not_exists(conn):
    exists = True
    try:
        conn.meta.client.head_bucket(Bucket=BUCKET_NAME)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            exists = False
    
    if exists == False:
        conn.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={
            'LocationConstraint': AWS_REGION})
        
def upload_to_s3(conn):    
    local_path = str(script_path.parents[1] / 'data').replace('\\', '/')
    for path, dirs, files in os.walk(local_path):
        for file in files:
            file_local = os.path.join(path, file).replace('\\', '/') 
            file_s3 = os.path.relpath(file_local, local_path).replace('\\', '/') 
            
            conn.meta.client.upload_file(
                Filename=file_local,
                Bucket=BUCKET_NAME,
                Key=f"{file_s3}" 
            )

if __name__ == '__main__':
    main()
        
