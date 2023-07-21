import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.models import Variable

PIWIK_CLIENT_ID = Variable.get('PIWIK_CLIENT_ID')
PIWIK_CLIENT_SECRET = Variable.get('PIWIK_CLIENT_SECRET')
PIWIK_WEBSITE_ID = Variable.get('PIWIK_WEBSITE_ID')

def generate_analytics_payload():

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday_pd = pd.to_datetime(yesterday).date().strftime('%Y-%m-%d')

    url = "https://ironhack.piwik.pro/auth/token"
    headers = {"Content-Type": "application/json"}

    data = {
        "grant_type": "client_credentials",
        "client_id": f'{PIWIK_CLIENT_ID}',
        "client_secret": f'{PIWIK_CLIENT_SECRET}'
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

    bearer = response.json()['access_token']

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday_pd = pd.to_datetime(yesterday).date().strftime('%Y-%m-%d')

    url = "https://ironhack.piwik.pro/api/analytics/v1/query/"

    payload = json.dumps({
        "date_from": f'{yesterday_pd}',
        "date_to":  f'{yesterday_pd}',
        "website_id": f'{PIWIK_WEBSITE_ID}',
        "offset": 0,
        "limit": 5000,
        "columns": [
            {
            "transformation_id": "to_date",
            "column_id": "timestamp"
            },
            {
            "transformation_id": "dimension_value_grouping",
            "dimension_value_grouping_id": "1f54fe13-1b24-46f2-8481-0167718d8bfa",
            "column_id": "location_country_name"
            },
            {
            "custom_channel_grouping_id": "67d495d6-9001-45f5-a2bd-df501f2c30d7",
            "column_id": "custom_channel_grouping"
            },
            {
            "column_id": "visitors"
            },
            {
            "column_id": "sessions"
            },
            {
            "calculated_metric_id": "e2c1033f-2849-448b-8044-83fc6ed79188",
            "column_id": "calculated_metric"
            }
        ],
        "order_by": [
            [
            0,
            "desc"
            ]
        ],
        "filters": {
            "operator": "and",
            "conditions": [
            {
                "operator": "or",
                "conditions": [
                {
                    "column_id": "referrer_url",
                    "condition": {
                    "operator": "not_matches",
                    "value": "vinted|blablacar|indeed|offerup|eurofirms"
                    }
                }
                ]
            },
            {
                "operator": "or",
                "conditions": [
                {
                    "column_id": "session_entry_url",
                    "condition": {
                    "operator": "not_matches",
                    "value": "impact|becas|schol|rappi|wallapop|uber|vinted|cabify|n26|kleider|accenture|landing-jobs|rebellion|eurofirms|eMerge|valencia|volvero|thomsonreuters|djiiga|EY|femhack| scholarships|tgtg|nextbike|cajoo|spielfeld|bnext|veepee|rastreotech|emerge|banco_23"
                    }
                }
                ]
            },
            {
                "operator": "or",
                "conditions": [
                {
                    "column_id": "campaign_content",
                    "condition": {
                    "operator": "not_matches",
                    "value": "scholarships|partnerships"
                    }
                }
                ]
            },
            {
                "operator": "or",
                "conditions": [
                {
                    "column_id": "location_country_name",
                    "condition": {
                    "operator": "neq",
                    "value": "IE"
                    }
                }
                ]
            },
            {
                "operator": "or",
                "conditions": [
                {
                    "column_id": "campaign_name",
                    "condition": {
                    "operator": "not_matches",
                    "value": "impact|becas|schol|rappi|wallapop|uber|vinted|cabify|n26|kleider|accenture|landing-jobs|rebellion|eurofirms|eMerge|valencia|volvero|thomsonreuters|djiiga|volvero|talent-pool-club|bipi|cajoo|freelance|spielfeld|scholarship-dutchdigitaltalent|talentland|trivago|percentil|lionstep|offerup"
                    }
                }
                ]
            },
            {
                "operator": "or",
                "conditions": [
                {
                    "column_id": "source",
                    "condition": {
                    "operator": "not_matches",
                    "value": "vinted|blablacar|indeed|offerup|amazon|sweatcoin|Klaviyo"
                    }
                }
                ]
            }
            ]
        },
        "metric_filters": None
        })

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {bearer}'
        }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def generate_events_payload():

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday_pd = pd.to_datetime(yesterday).date().strftime('%Y-%m-%d')

    url = "https://ironhack.piwik.pro/auth/token"
    headers = {"Content-Type": "application/json"}

    data = {
        "grant_type": "client_credentials",
        "client_id": f'{PIWIK_CLIENT_ID}',
        "client_secret": f'{PIWIK_CLIENT_SECRET}'
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

    bearer = response.json()['access_token']

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday_pd = pd.to_datetime(yesterday).date().strftime('%Y-%m-%d')

    url = "https://ironhack.piwik.pro/api/analytics/v1/events/" #by default --> visitorId, event_id, timestamp

    payload = json.dumps({
    "website_id": f'{PIWIK_WEBSITE_ID}',
    "columns": [
        {"column_id": "event_index"},
        {"column_id": "page_view_index" },
        {"column_id": "custom_event_category"},
        {"column_id": "custom_event_action"},
        {"column_id": "custom_event_name"},
        {"column_id": "event_url"},
        {"column_id": "source_medium"},
        {"column_id": "campaign_name"}
    ],
    "date_from": f'{yesterday_pd}',
    "date_to":  f'{yesterday_pd}',
    "filters": {
        "operator": "and",
        "conditions": []
    },
    "offset": 0,
    "limit": 5000,
    "format": "json"
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {bearer}'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def generate_sessions_payload():

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday_pd = pd.to_datetime(yesterday).date().strftime('%Y-%m-%d')

    url = "https://ironhack.piwik.pro/auth/token"
    headers = {"Content-Type": "application/json"}

    data = {
        "grant_type": "client_credentials",
        "client_id": f'{PIWIK_CLIENT_ID}',
        "client_secret": f'{PIWIK_CLIENT_SECRET}'
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

    bearer = response.json()['access_token']

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    yesterday_pd = pd.to_datetime(yesterday).date().strftime('%Y-%m-%d')

    url = "https://ironhack.piwik.pro/api/analytics/v1/sessions/" 

    payload = json.dumps({
    "website_id": f'{PIWIK_WEBSITE_ID}',
    "columns": [
        {
        "column_id": "visitor_session_number"
        },
        {
        "column_id": "source_medium" #could apply channel definition logics here to have the "session"
        },
        {
        "column_id": "campaign_name"
        },
        {
        "column_id": "session_goals" #this should be an array specifying the goals (apps, financing, etc)
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
        }
    ],
    "date_from": f'{yesterday_pd}',
    "date_to":  f'{yesterday_pd}',
    "filters": {
        "operator": "and",
        "conditions": []
    },
    "offset": 0,
    "limit": 500,
    "format": "json"
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {bearer}'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def transform_piwik_analytics(**kwargs):
            response = kwargs['ti'].xcom_pull(task_ids='piwik_analytics.load_piwik_analytics')
            column_names = response['meta']['columns']
            data = pd.DataFrame(columns = column_names)
            data = pd.DataFrame.from_dict(response['data'])
            ###
            column_names = ['date','country','channel','visitors','sessions','conversions']
            data.columns = column_names
            data['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')  
            data['visitors'] = data['visitors'].astype(int)
            data['sessions'] = data['sessions'].astype(int)
            data['conversions'] = data['conversions'].astype(int)
            data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
            return data

def transform_piwik_events(**kwargs):
            response = kwargs['ti'].xcom_pull(task_ids='piwik_events.load_piwik_events')
            column_names = response['meta']['columns']
            data = pd.DataFrame(columns = column_names)
            data = pd.DataFrame.from_dict(response['data'])
            data.columns = column_names
            ###
            data.replace([np.inf, -np.inf], 0, inplace=True)
            data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
            return data

def transform_piwik_sessions(**kwargs):
            response = kwargs['ti'].xcom_pull(task_ids='piwik_sessions.load_piwik_sessions')
            column_names = response['meta']['columns']
            data = pd.DataFrame(columns = column_names)
            data = pd.DataFrame.from_dict(response['data'])
            data.columns = column_names 
            ###
            data.replace([np.inf, -np.inf], 0, inplace=True)
            data = data.fillna(0)
            data['session_goals'] = [','.join(map(str, goal)) for goal in data['session_goals']]
            data['session_total_page_views'] = data['session_total_page_views'].astype(int)
            data['session_total_events'] = data['session_total_events'].astype(int)
            data['visitor_days_since_last_session'] = data['visitor_days_since_last_session'].astype(int)
            data['visitor_days_since_first_session'] = data['visitor_days_since_first_session'].astype(int)
            data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
            return data