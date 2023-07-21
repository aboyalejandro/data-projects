import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG   
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator 
from airflow.sensors.external_task import ExternalTaskSensor 
from export import ExportDestination   

with DAG(dag_id='attribution',
         description='joining attribution data on a daily basis',
         schedule_interval='@daily',
         start_date=datetime(2023, 7, 21, hour=1, minute=00, second=00)) as dag: 
    
    def postgre_to_df(**kwargs):
        hook = PostgresHook(postgres_conn_id="POSTGRESQL")
        df = hook.get_pandas_df(sql="""select prs.visitor_id,
            prs.session_id, 
            pre.event_id,
            pre.timestamp,
            pre.event_url,  
            pre.custom_event_category,
            pre.custom_event_action,
            pre.custom_event_name,
            prs.source_medium,
            prs.campaign_name,
            pre.event_index,
            pre.page_view_index,
            prs.visitor_session_number as touchpoint,
            pre.date_inserted 
        from piwik_pro_sessions_airflow prs 
        left join piwik_pro_events_airflow  pre 
            on prs.visitor_id  = pre.visitor_id
        where pre.timestamp::date = current_date-1""")
        return df

    postgre_to_df = PythonOperator(
        task_id='postgre_to_df',
        python_callable=postgre_to_df,
    )

    export = ExportDestination(
        task_id_xcom='postgre_to_df', 
        folder_path='attribution',
        postgres_table='attribution' 
    )   

    send_to_posgres = PythonOperator(task_id='send_to_posgres',
                            python_callable=export.send_to_posgres)

    upload_to_s3 = S3CreateObjectOperator(
                    task_id="upload_to_s3",
                    aws_conn_id= 'AWS_S3',
                    s3_bucket='piwik-pro-analytics',
                    s3_key = f'attribution/{pd.Timestamp.now().strftime("%y%m%d%H%M%S")}.parquet',
                    data="{{ ti.xcom_pull('postgre_to_df')}}"
                )
                
    postgre_to_df >> [send_to_posgres,upload_to_s3]