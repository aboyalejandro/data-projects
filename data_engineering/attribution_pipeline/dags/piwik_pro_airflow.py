import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG  
from airflow.decorators import task, dag, task_group 
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook 
from airflow.models import Variable 
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator 
from piwik import generate_analytics_payload, generate_sessions_payload, generate_events_payload
from piwik import transform_piwik_analytics, transform_piwik_events, transform_piwik_sessions
from export import ExportDestination  

with DAG(dag_id='piwik_pro',
         description='scraping piwik every day',
         schedule_interval='@daily',
         start_date=datetime(2023, 7, 21, hour=1, minute=00, second=00)) as dag: 
        
        with TaskGroup(group_id='piwik_sessions') as piwik_sessions:

            load_piwik_sessions = PythonOperator(task_id='load_piwik_sessions',
                                python_callable=generate_sessions_payload,    
                                trigger_rule="all_done")
            
            transform_piwik_sessions = PythonOperator(task_id='transform_piwik_sessions',
                            python_callable=transform_piwik_sessions,
                            trigger_rule="all_done")
            
            export_sessions = ExportDestination(
                    task_id_xcom='piwik_sessions.transform_piwik_sessions',  
                    folder_path='sessions',
                    postgres_table='piwik_pro_sessions_airflow'
                )   

            report_to_csv = PythonOperator(task_id='report_to_csv',
                                    python_callable=export_sessions.report_to_csv,
                                    trigger_rule="all_done")

            send_to_posgres = PythonOperator(task_id='send_to_posgres',
                                    python_callable=export_sessions.send_to_posgres,
                                    trigger_rule="all_done")

            upload_to_s3 = S3CreateObjectOperator(
                task_id="upload_to_s3",
                aws_conn_id= 'AWS_S3',
                s3_bucket='piwik-pro-analytics',
                s3_key = f'sessions/{pd.Timestamp.now().strftime("%y%m%d%H%M%S")}.parquet',
                data="{{ ti.xcom_pull('piwik_sessions.transform_piwik_sessions')}}"
            )
            
            load_piwik_sessions >> transform_piwik_sessions >> [report_to_csv,send_to_posgres,upload_to_s3]
        
        with TaskGroup(group_id='piwik_events') as piwik_events:
             
            load_piwik_events = PythonOperator(task_id='load_piwik_events',
                                python_callable=generate_events_payload,
                                trigger_rule="all_done")
            
            transform_piwik_events = PythonOperator(task_id='transform_piwik_events',
                                python_callable=transform_piwik_events,
                                trigger_rule="all_done")
            
            export_events = ExportDestination(
                task_id_xcom='piwik_events.transform_piwik_events', 
                folder_path='events',
                postgres_table='piwik_pro_events_airflow'
            )    

            report_to_csv = PythonOperator(task_id='report_to_csv',
                                    python_callable=export_events.report_to_csv,
                                    trigger_rule="all_done")
            
            send_to_posgres = PythonOperator(task_id='send_to_posgres',
                                    python_callable=export_events.send_to_posgres,
                                    trigger_rule="all_done")

            upload_to_s3 = S3CreateObjectOperator(
                task_id="upload_to_s3",
                aws_conn_id= 'AWS_S3',
                s3_bucket='piwik-pro-analytics',
                s3_key = f'events/{pd.Timestamp.now().strftime("%y%m%d%H%M%S")}.parquet',
                data="{{ ti.xcom_pull('piwik_events.transform_piwik_events')}}"
            )
            
            load_piwik_events >> transform_piwik_events >> [report_to_csv,send_to_posgres,upload_to_s3]
            
        with TaskGroup(group_id='piwik_analytics') as piwik_analytics:
            
            load_piwik_analytics = PythonOperator(task_id='load_piwik_analytics',
                                python_callable=generate_analytics_payload,
                                trigger_rule="all_done")
        
            transform_piwik_analytics = PythonOperator(task_id='transform_piwik_analytics',
                                python_callable=transform_piwik_analytics,
                                trigger_rule="all_done")
        
            export_analytics = ExportDestination(
                task_id_xcom='piwik_analytics.transform_piwik_analytics', 
                folder_path='analytics',
                postgres_table='piwik_pro_analytics_airflow'
            )   

            report_to_csv = PythonOperator(task_id='report_to_csv',
                                    python_callable=export_analytics.report_to_csv,
                                    trigger_rule="all_done")

            send_to_posgres = PythonOperator(task_id='send_to_posgres',
                                    python_callable=export_analytics.send_to_posgres,
                                    trigger_rule="all_done")

            upload_to_s3 = S3CreateObjectOperator(
                task_id="upload_to_s3",
                aws_conn_id= 'AWS_S3',
                s3_bucket='piwik-pro-analytics',
                s3_key = f'analytics/{pd.Timestamp.now().strftime("%y%m%d%H%M%S")}.parquet',
                data="{{ ti.xcom_pull('piwik_analytics.transform_piwik_analytics')}}"
            )
            
            load_piwik_analytics >> transform_piwik_analytics >> [report_to_csv,send_to_posgres,upload_to_s3]

        piwik_sessions >> piwik_analytics >> piwik_events 