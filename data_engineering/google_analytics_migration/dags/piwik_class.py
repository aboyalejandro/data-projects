import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import AirflowException
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator 

class Piwik:  

    def __init__(self, 
                 PIWIK_CLIENT_ID,
                 PIWIK_CLIENT_SECRET,
                 PIWIK_WEBSITE_ID,
                 PIWIK_AUTH_URL,
                 PIWIK_ANALYTICS_API,
                 PIWIK_EVENTS_API,
                 PIWIK_SESSIONS_API):
        
        self.PIWIK_CLIENT_ID = PIWIK_CLIENT_ID
        self.PIWIK_CLIENT_SECRET = PIWIK_CLIENT_SECRET
        self.PIWIK_WEBSITE_ID = PIWIK_WEBSITE_ID
        self.PIWIK_AUTH_URL = PIWIK_AUTH_URL
        self.PIWIK_ANALYTICS_API = PIWIK_ANALYTICS_API
        self.PIWIK_EVENTS_API = PIWIK_EVENTS_API
        self.PIWIK_SESSIONS_API = PIWIK_SESSIONS_API

    def auth(self,**kwargs):
        url = f'{self.PIWIK_AUTH_URL}'
        headers = {"Content-Type": "application/json"}

        data = {
                "grant_type": "client_credentials",
                "client_id": f'{self.PIWIK_CLIENT_ID}',
                "client_secret": f'{self.PIWIK_CLIENT_SECRET}'
            }

        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            bearer = response.json()['access_token']
            kwargs['ti'].xcom_push(key = 'auth' , value = bearer)
            print('Bearer token accepted.')
            return bearer 
        else:
            raise AirflowException('Auth invalid.')
        
    def generate_analytics_payload(self,**kwargs):
            
            url = f'{self.PIWIK_ANALYTICS_API}'
            bearer = kwargs['ti'].xcom_pull(key='auth') 

            payload = json.dumps({
            "date_from": kwargs.get('ds'),
            "date_to": kwargs.get('ds'),
            "website_id": f'{self.PIWIK_WEBSITE_ID}',
            "offset": 0,
            "limit": 10000,
            "columns": [
                {
                "transformation_id": "to_date",
                "column_id": "timestamp"
                },
                {
                    
                    "column_id": "referrer_type"
                },
                {
                    "column_id": "source_medium"
                },
                {
                    "column_id": "campaign_name"
                },
                {
                    "column_id": "campaign_content"
                },
                {
                    "column_id": "session_entry_url"
                },
                {
                    "column_id": "visitor_returning"
                },
                {
                    "column_id": "location_country_name"
                },
                {
                    "column_id": "operating_system"
                },
                {
                    "column_id": "device_type"
                },
                {
                    "column_id": "events"
                },
                {
                    "column_id": "visitors"
                },
                {
                    "column_id": "sessions"
                },
                {
                    "column_id": "page_views"
                },
                {
                    "column_id": "ecommerce_conversions"
                },
                {
                    "column_id": "cart_additions"
                },
                {
                    "column_id": "ecommerce_abandoned_carts"
                },
                {
                    "column_id": "consents_none"
                },
                {
                    "column_id": "consents_full"
                }
            ]
            })
            headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {bearer}'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            return response.json()

    def generate_events_payload(self,**kwargs):

            url = f'{self.PIWIK_EVENTS_API}'
            bearer = kwargs['ti'].xcom_pull(key='auth') 

            payload = json.dumps({
                "website_id": f'{self.PIWIK_WEBSITE_ID}',
                "columns": [
                    {"column_id": "event_index"},
                    {"column_id": "page_view_index" },
                    {"column_id": "custom_event_category"},
                    {"column_id": "custom_event_action"},
                    {"column_id": "custom_event_name"},
                    {"column_id": "event_url"},
                    {"column_id": "source_medium"},
                    {"column_id": "campaign_name"},
                    {"column_id":"goal_id"}
                ],
                "date_from": kwargs.get('ds'),
                "date_to": kwargs.get('ds'),
                "filters": {
                    "operator": "and",
                    "conditions": []
                },
                "offset": 0,
                "limit": 10000,
                "format": "json"
                })
            headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {bearer}'
            }
            
            response = requests.request("POST", url, headers=headers, data=payload)
            return response.json()

    def generate_sessions_payload(self,**kwargs):

            url = f'{self.PIWIK_SESSIONS_API}'
            bearer = kwargs['ti'].xcom_pull(key='auth') 

            payload = json.dumps({
                "website_id": f'{self.PIWIK_WEBSITE_ID}',
                "columns": [
                    {
                    "column_id": "visitor_session_number"
                    },
                    {
                    "column_id": "visitor_returning"
                    },
                    {
                    "column_id": "source_medium" #could apply channel definition logics here to have the "session"
                    },
                    {
                    "column_id": "campaign_name"
                    },
                    {
                    "column_id": "session_total_page_views"
                    },
                    {
                    "column_id": "session_total_events"
                    },
                    {
                    "column_id": "visitor_days_since_last_session"
                    },
                    {
                    "column_id": "visitor_days_since_first_session"
                    },
                    {
                    "column_id": "location_country_name"
                    },
                ],
                "date_from": kwargs.get('ds'),
                    "date_to": kwargs.get('ds'),
                "filters": {
                    "operator": "and",
                    "conditions": []
                },
                "offset": 0,
                "limit": 10000,
                "format": "json"
                })
            headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {bearer}'
            }
            
            response = requests.request("POST", url, headers=headers, data=payload)
            return response.json()

    def transform_piwik_sessions(self,**kwargs):
                response = kwargs['ti'].xcom_pull(task_ids='piwik_sessions.load_piwik_sessions')
                data = pd.DataFrame.from_dict(response['data'])
                data.columns = response['meta']['columns']
                #
                data['visitor_returning'] = data['visitor_returning'].apply(lambda x: x[1])
                data['location_country_name'] = data['location_country_name'].apply(lambda x: x[1])
                #
                data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
                kwargs['ti'].xcom_push(key = 'final_data_sessions' , value = data.to_csv(index=False))
                if data.shape[0] > 0:
                    return 'Dataframe has values.'
                else: 
                    return 'Empty dataframe.'
    
    def transform_piwik_analytics(self,**kwargs):
                response = kwargs['ti'].xcom_pull(task_ids='piwik_analytics.load_piwik_analytics')
                data = pd.DataFrame.from_dict(response['data'])
                data.columns = response['meta']['columns']
                #
                data['referrer_type'] = data['referrer_type'].apply(lambda x: x[1])
                data['visitor_returning'] = data['visitor_returning'].apply(lambda x: x[1])
                data['location_country_name'] = data['location_country_name'].apply(lambda x: x[1])
                data['operating_system'] = data['operating_system'].apply(lambda x: x[1])
                data['device_type'] = data['device_type'].apply(lambda x: x[1])
                #
                data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
                kwargs['ti'].xcom_push(key = 'final_data_analytics' , value = data.to_csv(index=False))
                if data.shape[0] > 0:
                    return 'Dataframe has values.'
                else: 
                    return 'Empty dataframe.'

    def transform_piwik_events(self,**kwargs):
                response = kwargs['ti'].xcom_pull(task_ids='piwik_events.load_piwik_events')
                data = pd.DataFrame.from_dict(response['data'])
                data.columns = response['meta']['columns']
                #
                data['goal_id'] = data['goal_id'].apply(lambda x: x[1])
                #
                data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
                kwargs['ti'].xcom_push(key = 'final_data_events' , value = data.to_csv(index=False))
                if data.shape[0] > 0:
                    return 'Dataframe has values.'
                else: 
                    return 'Empty dataframe.'

    