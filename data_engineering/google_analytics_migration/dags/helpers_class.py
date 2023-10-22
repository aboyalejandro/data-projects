# Wrap Up version (should be used with decorator)
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import AirflowException
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator 

class Helpers:  

    def __init__(self, 
                 TASK_ID=None,
                 TASK_GROUP_ID = None,
                 S3_BUCKET=None,
                 S3_FOLDER=None,
                 FILE_FORMAT=None):
        
        self.TASK_ID = TASK_ID
        self.TASK_GROUP_ID = TASK_GROUP_ID
        self.S3_BUCKET = S3_BUCKET
        self.S3_FOLDER = S3_FOLDER
        self.FILE_FORMAT = FILE_FORMAT

    def transform_payload(self,**kwargs):
            response = kwargs['ti'].xcom_pull(task_ids=f'{self.TASK_GROUP_ID}.{self.TASK_ID}')
            data = pd.DataFrame.from_dict(response['data'])
            data.columns = response['meta']['columns']
            data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
            kwargs['ti'].xcom_push(key = 'final_data' , value = f'data.to_{self.FILE_FORMAT.str.lower()}(index=False))')
            if data.shape[0] > 0:
                return 'Dataframe has values.'
            else: 
                return 'Empty dataframe.'
                    
    def send_to_s3_bucket(self,**kwargs):
            
            s3_key=f'{self.S3_FOLDER}/{{task_instance_key_str}}.{self.FILE_FORMAT.str.lower()}'

            upload_to_s3 = S3CreateObjectOperator(
                    task_id="upload_to_s3",
                    aws_conn_id= 'AWS_S3',
                    s3_bucket=f'{self.S3_BUCKET}',
                    s3_key = s3_key,
                    data="{{ ti.xcom_pull(key='final_data') }}"
            ) 