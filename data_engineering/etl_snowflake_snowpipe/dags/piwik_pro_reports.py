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
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from piwik_class import Piwik
from helpers_class import Helpers

# PIWIK
PIWIK_CLIENT_ID = Variable.get('PIWIK_CLIENT_ID')
PIWIK_CLIENT_SECRET = Variable.get('PIWIK_CLIENT_SECRET') 
PIWIK_WEBSITE_ID = Variable.get('PIWIK_WEBSITE_ID')
PIWIK_AUTH_URL = Variable.get('PIWIK_AUTH_URL') 
PIWIK_ANALYTICS_API = Variable.get('PIWIK_ANALYTICS_API')
PIWIK_EVENTS_API = Variable.get('PIWIK_EVENTS_API')  
PIWIK_SESSIONS_API = Variable.get('PIWIK_SESSIONS_API')
PIWIK_REQUEST_LIMIT = 10000
# PENDING A FILE_EXTENSION PARAM

# AWS
AWS_CONN_ID = "AWS_S3"
AWS_KEY_ID = Variable.get('AWS_KEY_ID')  
AWS_SECRET_KEY = Variable.get('AWS_SECRET_KEY')  
S3_BUCKET = "piwik-pro-analytics"
S3_KEY_EXTENSION = ".csv" # Change to Parquet

# SNOWFLAKE
SNOWFLAKE_SCHEMA = "RAW"
FILE_FORMAT = "RAW_SNOWFLAKE.piwik_pro.csv"
STAGE_SCHEMA = "external_stages"


piwik = Piwik(PIWIK_CLIENT_ID,
                      PIWIK_CLIENT_SECRET,
                      PIWIK_WEBSITE_ID,
                      PIWIK_AUTH_URL,
                      PIWIK_ANALYTICS_API,
                      PIWIK_EVENTS_API,
                      PIWIK_SESSIONS_API,
                      PIWIK_REQUEST_LIMIT
                      ) 

with DAG(dag_id='piwik_pro', 
         description='scraping piwik pro API from demo account every day',
         schedule='@daily',
         start_date=datetime(2021, 1, 1),
         end_date=datetime(2021, 1, 1)) as dag:  
        
        piwik_auth = PythonOperator(task_id='piwik_auth',
                                python_callable=piwik.auth,
                                provide_context = True     
        )        

        # DEFINE REPEATABLE LOOP 
             
        with TaskGroup(group_id='piwik_sessions') as piwik_sessions:

            TASK_GROUP = "sessions"

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
                aws_conn_id= AWS_CONN_ID,
                s3_bucket=S3_BUCKET,
                s3_key = f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                data="{{ ti.xcom_pull(key='final_data_sessions') }}"   

            )

            s3_key_sensor = S3KeySensor( 
                task_id='s3_key_sensor',
                aws_conn_id=AWS_CONN_ID,
                bucket_name=S3_BUCKET,
                bucket_key=f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                timeout=600,
                poke_interval=60, 
                dag=dag,
            )

            create_snowflake_stage = SnowflakeOperator(
                task_id='create_snowflake_stage',
                sql=f""" USE SCHEMA {STAGE_SCHEMA};
                
                        CREATE OR REPLACE STAGE piwik_{TASK_GROUP}
                        URL = 's3://{S3_BUCKET}/{TASK_GROUP}/'
                        CREDENTIALS = (AWS_KEY_ID = {AWS_KEY_ID} 
                                       AWS_SECRET_KEY = {AWS_SECRET_KEY})
                        FILE_FORMAT = {FILE_FORMAT};
                    """,
                snowflake_conn_id='SNOWFLAKE', 
                autocommit=True,
                dag=dag,
            )

            copy_into_snowflake_table = S3ToSnowflakeOperator(
                task_id='copy_into_snowflake_table',
                snowflake_conn_id='SNOWFLAKE',
                s3_keys=f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                table=TASK_GROUP,
                schema=SNOWFLAKE_SCHEMA,
                stage=f'piwik_{TASK_GROUP}',
                file_format=FILE_FORMAT,
                dag=dag,
            )
            
            load_piwik_sessions >> transform_piwik_sessions >> upload_to_s3 >> s3_key_sensor >> create_snowflake_stage >> copy_into_snowflake_table
             
        with TaskGroup(group_id='piwik_events') as piwik_events:

            TASK_GROUP = "events"
             
            load_piwik_events = PythonOperator(task_id='load_piwik_events',
                                python_callable=piwik.generate_events_payload
            )
            
            transform_piwik_events = PythonOperator(task_id='transform_piwik_events',
                                python_callable=piwik.transform_piwik_events
            )

            upload_to_s3 = S3CreateObjectOperator( 
                task_id="upload_to_s3",
                aws_conn_id= AWS_CONN_ID,
                s3_bucket=S3_BUCKET,
                s3_key = f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                data="{{ ti.xcom_pull(key='final_data_events') }}"
            )

            s3_key_sensor = S3KeySensor( 
                task_id='s3_key_sensor',
                aws_conn_id=AWS_CONN_ID,
                bucket_name=S3_BUCKET,
                bucket_key=f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                timeout=600,
                poke_interval=60, 
                dag=dag,
            )

            create_snowflake_stage = SnowflakeOperator(
                task_id='create_snowflake_stage',
                sql=f""" USE SCHEMA {STAGE_SCHEMA};
                
                        CREATE OR REPLACE STAGE piwik_{TASK_GROUP}
                        URL = 's3://{S3_BUCKET}/{TASK_GROUP}/'
                        CREDENTIALS = (AWS_KEY_ID = {AWS_KEY_ID} 
                                       AWS_SECRET_KEY = {AWS_SECRET_KEY})
                        FILE_FORMAT = {FILE_FORMAT};
                    """,
                snowflake_conn_id='SNOWFLAKE', 
                autocommit=True,
                dag=dag,
            )

            copy_into_snowflake_table = S3ToSnowflakeOperator(
                task_id='copy_into_snowflake_table',
                snowflake_conn_id='SNOWFLAKE',
                s3_keys=f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                table=TASK_GROUP,
                schema=SNOWFLAKE_SCHEMA,
                stage=f'piwik_{TASK_GROUP}',
                file_format=FILE_FORMAT,
                dag=dag,
            )
            
            load_piwik_events >> transform_piwik_events >> upload_to_s3 >> s3_key_sensor >> create_snowflake_stage >> copy_into_snowflake_table
            
        with TaskGroup(group_id='piwik_analytics') as piwik_analytics:

            TASK_GROUP = "analytics"
            
            load_piwik_analytics = PythonOperator(task_id='load_piwik_analytics',
                                python_callable=piwik.generate_analytics_payload
            )
        
            transform_piwik_analytics = PythonOperator(task_id='transform_piwik_analytics',
                                python_callable=piwik.transform_piwik_analytics 
            )

            upload_to_s3 = S3CreateObjectOperator(
                task_id="upload_to_s3",
                aws_conn_id= AWS_CONN_ID, 
                s3_bucket=S3_BUCKET,
                s3_key = f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                data="{{ ti.xcom_pull(key='final_data_analytics') }}"
            ) 

            s3_key_sensor = S3KeySensor( 
                task_id='s3_key_sensor',
                aws_conn_id=AWS_CONN_ID,
                bucket_name=S3_BUCKET,
                bucket_key=f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                timeout=600,
                poke_interval=60, 
                dag=dag, 
            )

            create_snowflake_stage = SnowflakeOperator(
                task_id='create_snowflake_stage',
                sql=f""" USE SCHEMA {STAGE_SCHEMA};
                
                        CREATE OR REPLACE STAGE piwik_{TASK_GROUP}
                        URL = 's3://{S3_BUCKET}/{TASK_GROUP}/'
                        CREDENTIALS = (AWS_KEY_ID = {AWS_KEY_ID} 
                                       AWS_SECRET_KEY = {AWS_SECRET_KEY})
                        FILE_FORMAT = {FILE_FORMAT};
                    """,
                snowflake_conn_id='SNOWFLAKE', 
                autocommit=True,
                dag=dag,
            )

            copy_into_snowflake_table = S3ToSnowflakeOperator(
                task_id='copy_into_snowflake_table',
                snowflake_conn_id='SNOWFLAKE',
                s3_keys=f'{TASK_GROUP}/{{{{ ds_nodash }}}}{S3_KEY_EXTENSION}',
                table=TASK_GROUP,
                schema=SNOWFLAKE_SCHEMA,
                stage=f'piwik_{TASK_GROUP}',
                file_format=FILE_FORMAT,
                dag=dag,
            )
            
            load_piwik_analytics >> transform_piwik_analytics >> upload_to_s3 >> s3_key_sensor >> create_snowflake_stage >> copy_into_snowflake_table

        piwik_auth >> [piwik_sessions,piwik_analytics,piwik_events] 