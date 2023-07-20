import os
import sys
import zipfile
import subprocess
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from google.cloud import storage



DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BUCKET_NAME = Variable.get("BUCKET_NAME")
GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
BIGQUERY_TABLE_NAME = "online_payment"
GCS_OBJECT_NAME = "online_payment.csv"
DATA_PATH = f"{BASE_PATH}/datasets"
OUT_PATH = f"{DATA_PATH}/{GCS_OBJECT_NAME}"

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB 

def download_from_gdrive():
      url = 'https://drive.google.com/u/0/uc?id=1ZMBiOBTk-KnVJEegdqfk3pLgmccnUiTF&confirm=t'
      output = f'{DATA_PATH}/online_payment.zip'
      subprocess.run(['curl', '-L', url, '-o', output])

def unzip_file():      
      with zipfile.ZipFile(f'{DATA_PATH}/online_payment.zip', 'r') as zip_ref:
        zip_ref.extractall(f'{DATA_PATH}')
      old_name = f'{DATA_PATH}/PS_20174392719_1491204439457_log.csv'
      new_name = f'{DATA_PATH}/online_payment.csv'
      os.rename(old_name, new_name)

def extract_transform():
      df = pd.read_csv(f"{DATA_PATH}/online_payment.csv")
      columns = ['setp', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest', 'oldbalanceDest']
      df.to_csv(OUT_PATH, index=False, header=False)

default_args = {
     "owner" : "kelompok 4 airflow",
     "start_date" : days_ago(1),
     "depends_on_past" : False,
     "retries" : 1
}

with DAG(
     dag_id="final_project_df10",
     schedule_interval=None,
     default_args=default_args,
     catchup=False,
     tags=["online_payment", "final_project", "kelompok4", "df10"]
) as dag:
    
    start = DummyOperator(task_id='start')

    download_from_gdrive_task = PythonOperator(
         task_id="download_from_gdrive_task",
         python_callable=download_from_gdrive
    ) 
    
    unzip_file_task = PythonOperator(
         task_id="unzip_file_task",
         python_callable=unzip_file
    )

    extract_transform_task = PythonOperator(
         task_id="extract_transform_task",
         python_callable=extract_transform
    )

    stored_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_to_gcs",
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        src=OUT_PATH,
        dst=GCS_OBJECT_NAME,
        bucket=BUCKET_NAME
    )

    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'step', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'amount', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'nameOrig', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'oldbalanceOrg', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'newbalanceOrig', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'nameDest', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'oldbalanceDest', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'newbalanceDest', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'isFraud', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'isFlaggedFraud', 'type': 'INT64', 'mode': 'NULLABLE'},
        ], 
        autodetect=False,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV' #If the table already exists - overwrites the table data
    )
    end = DummyOperator(task_id='end')


    start >> download_from_gdrive_task >> \
    unzip_file_task >> extract_transform_task >> \
    stored_data_gcs >> loaded_data_bigquery >> end

