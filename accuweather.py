from accuweather_key import accuweather_api_key
import pandas as pd
import requests
import time


# Import persisted data

today_data = pd.read_pickle('today_data.pickle')
tomorrow_data = pd.read_pickle('tomorrow_data.pickle')



# Query API for location codes for each unique city

cities = set(today_data['city'])

for city in cities:
    
    location_url = f'http://dataservice.accuweather.com/locations/v1/cities/US/search?apikey={accuweather_api_key}&q={city}'
    headers = {'Accept-Encoding': 'gzip'}

    retries = 3
    for i in range(retries):
        try:
            response = requests.get(location_url, headers=headers, timeout=3.1)
            response.raise_for_status()
            break
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(i + 1)
            continue
        except requests.HTTPError:
            if response.status_code == 500:
                time.sleep(i + 1)
                continue

    location_data = response.json()
    location_key = location_data[0]['Key']



    # Query API for air quality per location

    url = f'http://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?apikey={accuweather_api_key}&details=true'

    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=3.1)
            response.raise_for_status()
            break
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(i + 1)
            continue
        except requests.HTTPError:
            if response.status_code == 500:
                time.sleep(i + 1)
                continue

    data = response.json()
    today = data['DailyForecasts'][0]['AirAndPollen']
    tomorrow = data['DailyForecasts'][1]['AirAndPollen']

    # List of park_ids matching this city; may be multiple
    parks_in_city = list(today_data.query('city == @city').index)

    for p in parks_in_city:
        #Insert to today_data

        for datum in today:
            name = datum['Name']
            if name == 'AirQuality':
                today_data.loc[p, 'air_quality_type'] = datum['Type']
                today_data.loc[p, 'air_quality_level'] = datum['Category']
            if name == 'Grass':
                today_data.loc[p, 'pollen_grass'] = datum['Category']
            if name == 'Mold':
                today_data.loc[p, 'pollen_mold'] = datum['Category']
            if name == 'Ragweed':
                today_data.loc[p, 'pollen_ragweed'] = datum['Category']
            if name == 'Tree':
                today_data.loc[p, 'pollen_tree'] = datum['Category']

        #Insert to tomorrow_data

        for datum in tomorrow:
            name = datum['Name']
            if name == 'AirQuality':
                tomorrow_data.loc[p, 'air_quality_type'] = datum['Type']
                tomorrow_data.loc[p, 'air_quality_level'] = datum['Category']
            if name == 'Grass':
                tomorrow_data.loc[p, 'pollen_grass'] = datum['Category']
            if name == 'Mold':
                tomorrow_data.loc[p, 'pollen_mold'] = datum['Category']
            if name == 'Ragweed':
                tomorrow_data.loc[p, 'pollen_ragweed'] = datum['Category']
            if name == 'Tree':
                tomorrow_data.loc[p, 'pollen_tree'] = datum['Category']



# Persist the data

today_data.to_pickle('today_data.pickle')
tomorrow_data.to_pickle('tomorrow_data.pickle')