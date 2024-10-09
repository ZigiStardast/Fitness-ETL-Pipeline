# Fitness-ETL-Pipeline
This project implements a complete ETL (Extract, Transform, Load) pipeline using a variety of technologies to process and analyze fitness data.

## Architecture
![workflow 2](https://github.com/user-attachments/assets/29db6618-44cd-45a4-a592-0b7c4a9a4a69)

## Set-up
Download / clone the repo

Configure AWS Credentials: Ensure your AWS credentials are available in the .aws/credentials
```
[default]
aws_access_key_id = 
aws_secret_access_key = 
aws_session_token = 
```

Use Terraform scripts to deploy the necessary AWS resources.
```
cd fitness-data-pipeline/terraform/
```
```
terraform init
terraform apply
```

Load data to aws s3 bucket:
```
cd fitness-data-pipeline/airflow/scripts/
python load_to_s3.py
```

Run docker-compose file:
```
cd fitness-data-pipeline/airflow/
```
```
docker-compose up -d
```
This command will pull and create Docker images and containers for Airflow, according to the instructions in the docker-compose.yaml.

Access the Airflow web interface by going to http://localhost:8080/, the default user is <b>airflow</b> and password is <b>airflow</b>.
Once you’ve signed in, the Airflow homepage displays a list of all available DAGs, including your own and any example DAGs provided by Airflow, sorted in alphabetical order. Any Python script for a DAG saved in the dags/ directory will automatically appear on this page.
<b>fitness-dag<b/> is the one built for this project.

Trigger a DAG Run.

In the Graph View page, once the DAG has been unpaused and triggered, you can watch its progress as it moves through each task.
## Pipeline Tasks

## Prerequisites
• AWS account with S3, Redshift, and IAM permissions \
• Docker \
• Terraform \
• Python with necessary libraries \
• Google Data Studio account

