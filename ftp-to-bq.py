#!/usr/bin/env python3
#----------------------------------------------------------------------------
# Created By  : rathanpv1997 
# ---------------------------------------------------------------------------
""" Airflow Dag to load multiple tables data dumps from from ftp server to Bigquery using GCS as staging layer and Dataflow to copy from GCS to BQ"""  #Line 4

from google.cloud import storage
import airflow
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
import json

default_args = {
    'owner': 'airflow',
    # 'start_date': airflow.utils.dates.days_ago(0),
    'start_date': datetime(2021, 2, 7),
    'retries': 2,
    'retry_delay':timedelta(minutes=2),
    'email_on_retry': False,
    'dataflow_default_options': {
        'project': 'ags-data-pilot',
        'zone': 'us-east1-d',
        'tempLocation': 'gs://bucket-name/temp',
        'subnetwork': "regions/us-east1/subnetworks/subnet-name",
        'maxWorkers': 2,
        'numWorkers': 1,
        'machineType': "n1-standard-2",
        "disk_size_gb":20,
        "service_account_email":"service-account@projectid.iam.gserviceaccount.com"
         },
}
# execution_date=kwargs.get('ts_nodash')

dag = DAG(dag_id="ftp_to_bq_dag",schedule_interval='0 12 * * *',catchup=False,default_args=default_args)

SSH_CONN_ID = "ssh_default"
move_files_from_sftp_to_gcs = SSHOperator(
    task_id="file-move-sftp-to-gcs-destination",
    ssh_conn_id=SSH_CONN_ID,
    command='gsutil -m cp -R /home/ftp-user/upload/* gs://bucket-name/upload/{{ds}}/',
    dag=dag,
)


tables_dict={'table1_ftp':'table1','table2_ftp':'table2'}

for file_name,table_name in tables_dict.items():

    dataflow_cloudstorage_to_Bq = DataflowTemplateOperator(
    task_id='dataflow_cloudstorage_to_Bq'+table_name,
    job_name='gstext_to_bq',
    template='gs://dataflow-templates-us-east1/latest/GCS_Text_to_BigQuery',
    parameters={
            "javascriptTextTransformFunctionName": "transform",
            "JSONPath": "gs://bucket-name/dataflowcodes/"+table_name+"_schema.json",
            "javascriptTextTransformGcsPath": "gs://bucket-name/dataflowcodes/"+table_name+".js",
            "inputFilePattern":"gs://bucket-name/upload/{{ds}}/"+file_name+".csv",
            "outputTable":"project-id:dataset-id."+table_name,
            "bigQueryLoadingTemporaryDirectory": "gs://bucket-name/temp",
    },
    location='us-east1',
    dag=dag
    )

    move_files_from_sftp_to_gcs  >> dataflow_cloudstorage_to_Bq
