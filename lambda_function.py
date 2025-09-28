from datetime import datetime, timedelta, timezone
import os
import json
import boto3
import pandas as pd
import io
import requests
from datetime import datetime, timezone
from geopy.geocoders import Nominatim

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'weather-project-raw'
    geolocator = Nominatim(user_agent='weather_data_project_practice')
    api_key = os.environ['API_KEY']

    #establish time period to pull polution history. previous 5 day interval
    current_time = datetime.now(timezone.utc)
    end_period = current_time.replace(hour=12, minute=0, second=0, microsecond=0)
    start_period = (current_time - timedelta(days = 4)).replace(hour=11, minute=0, second=0, microsecond=0)
    end_period_unifix = str(int(end_period.timestamp()))
    start_period_unifix = str(int(start_period.timestamp()))

    #target cities
    cities = ['London', 'New York', 'Cape Town']

    #lat & lon required for pollution api
    city_location = {}
    for city in cities:
        location = geolocator.geocode(city)
        city_location[city] = [location.latitude, location.longitude]

    #weather data pull
    for city in cities:
        URL = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}'
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()

            datestamp = data['list'][0]['dt_txt']
            datestamp = datestamp.replace('-','').replace(' ','')
            datestamp = datestamp[:8]
            city = city.replace(' ','_')
            s3_key = f'data/{datestamp}_{city}.json'

            json_buffer = io.StringIO()
            json.dump(data, json_buffer)

            s3.put_object(
            Bucket = bucket_name,
            Key = s3_key,
            Body = json_buffer.getvalue()
            )
            print(f'Added file {datestamp}_{city}.json')

    #pollution data pull
    for city in cities:
        lat = city_location[city][0]
        lon = city_location[city][1]
        URL = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_period_unifix}&end={end_period_unifix}&appid={api_key}'
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            datestamp = start_period.strftime("%Y%m%d")
            city = city.replace(' ','_')
            s3_key = f'pollution_data/{datestamp}_{city}_pollution.json'
        
            json_buffer = io.StringIO()
            json.dump(data, json_buffer)

            s3.put_object(
            Bucket = bucket_name,
            Key = s3_key,
            Body = json_buffer.getvalue()
            )
            print(f'Added file {datestamp}_{city}_pollution.json')