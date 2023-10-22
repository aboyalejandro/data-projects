import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG  
from airflow.decorators import task, dag, task_group 
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models import Variable 
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator 
from piwik_class import Piwik
from helpers_class import Helpers

PIWIK_CLIENT_ID = Variable.get('PIWIK_CLIENT_ID')
PIWIK_CLIENT_SECRET = Variable.get('PIWIK_CLIENT_SECRET') 
PIWIK_WEBSITE_ID = Variable.get('PIWIK_WEBSITE_ID')
PIWIK_AUTH_URL = Variable.get('PIWIK_AUTH_URL') 
PIWIK_ANALYTICS_API = Variable.get('PIWIK_ANALYTICS_API')
PIWIK_EVENTS_API = Variable.get('PIWIK_EVENTS_API')  
PIWIK_SESSIONS_API = Variable.get('PIWIK_SESSIONS_API')

with DAG(dag_id='piwik_pro', 
         description='scraping piwik pro API from demo account every day',
         schedule_interval='@daily',
         start_date=datetime(2021, 1, 1),
         end_date=datetime(2021, 3, 30)) as dag:  

        piwik = Piwik(PIWIK_CLIENT_ID,
                      PIWIK_CLIENT_SECRET,
                      PIWIK_WEBSITE_ID,
                      PIWIK_AUTH_URL,
                      PIWIK_ANALYTICS_API,
                      PIWIK_EVENTS_API,
                      PIWIK_SESSIONS_API
                      )

        piwik_auth = PythonOperator(task_id='piwik_auth',
                                python_callable=piwik.auth,
                                provide_context = True     
        )        
          
        with TaskGroup(group_id='piwik_sessions') as piwik_sessions:

            load_piwik_sessions = PythonOperator(task_id='load_piwik_sessions',
                                python_callable=piwik.generate_sessions_payload,
                                provide_context = True    
            )
            
            transform_piwik_sessions = PythonOperator(task_id='transform_piwik_sessions',
                            python_callable=piwik.transform_piwik_sessions,
                            provide_context = True 
            )

            upload_to_s3 = S3CreateObjectOperator(
                task_id="upload_to_s3",
                aws_conn_id= 'AWS_S3',
                s3_bucket='piwik-pro-analytics',
                s3_key = 'sessions/{{task_instance_key_str}}.csv',
                data="{{ ti.xcom_pull(key='final_data_sessions') }}"  
                
            )
            
            load_piwik_sessions >> transform_piwik_sessions >> upload_to_s3
             
        with TaskGroup(group_id='piwik_events') as piwik_events:
             
            load_piwik_events = PythonOperator(task_id='load_piwik_events',
                                python_callable=piwik.generate_events_payload
            )
            
            transform_piwik_events = PythonOperator(task_id='transform_piwik_events',
                                python_callable=piwik.transform_piwik_events
            )

            upload_to_s3 = S3CreateObjectOperator( 
                task_id="upload_to_s3",
                aws_conn_id= 'AWS_S3',
                s3_bucket='piwik-pro-analytics',
                s3_key ='events/{{task_instance_key_str}}.csv',
                data="{{ ti.xcom_pull(key='final_data_events') }}"
            )
            
            load_piwik_events >> transform_piwik_events >> upload_to_s3
            
        with TaskGroup(group_id='piwik_analytics') as piwik_analytics:
            
            load_piwik_analytics = PythonOperator(task_id='load_piwik_analytics',
                                python_callable=piwik.generate_analytics_payload
            )
        
            transform_piwik_analytics = PythonOperator(task_id='transform_piwik_analytics',
                                python_callable=piwik.transform_piwik_analytics 
            )

            upload_to_s3 = S3CreateObjectOperator(
                task_id="upload_to_s3",
                aws_conn_id= 'AWS_S3',
                s3_bucket='piwik-pro-analytics',
                s3_key = 'analytics/{{task_instance_key_str}}.csv',
                data="{{ ti.xcom_pull(key='final_data_analytics') }}"
            ) 
            
            load_piwik_analytics >> transform_piwik_analytics >> upload_to_s3

        piwik_auth >> [piwik_sessions,piwik_analytics,piwik_events] 