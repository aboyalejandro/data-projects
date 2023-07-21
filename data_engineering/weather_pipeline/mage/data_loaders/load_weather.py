import pandas as pd
import requests
import time
from datetime import date
import warnings
warnings.filterwarnings('ignore')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    key = '9469c3a6ee9b431dba2170951231906'

    cities = ['Madrid','Barcelona','London','Buenos Aires','Lisbon','New York','Sidney','Los Angeles','Berlin','Oslo','Santiago de Chile','Montevideo','Amsterdam','Paris']

    column_names = ['name','country','latitude','longitud','timezone','local_time','temperature','is_day','condition','wind_speed','pressure','humidity','uv']
    weather = pd.DataFrame(columns = column_names)

    for city in cities:

        url = f'http://api.weatherapi.com/v1/current.json?key={key}&q={city}'
        response = requests.get(url)
        data = response.json()

        new_weather_report = {'date': date.today(),
                                    'name': data['location']['name'],
                                    'country': data['location']['country'],
                                    'latitude':data['location']['lat'],
                                    'longitud':data['location']['lon'],
                                    'timezone':data['location']['tz_id'],
                                    'local_time': data['location']['localtime'],
                                    'temperature':data['current']['temp_c'],
                                    'is_day':data['current']['is_day'],
                                    'condition': data['current']['condition']['text'],
                                    'wind_speed':data['current']['wind_mph'],
                                    'pressure': data['current']['pressure_mb'],
                                    'humidity': data['current']['humidity'],
                                    'uv': data['current']['uv']
                                }

        weather = weather.append(new_weather_report, ignore_index=True)
        
    return weather

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
