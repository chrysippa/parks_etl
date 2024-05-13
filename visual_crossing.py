import csv
import pandas as pd
import requests
from google.cloud import secretmanager
import time


# Import persisted data

today_data = pd.read_pickle('today_data.pickle')
tomorrow_data = pd.read_pickle('tomorrow_data.pickle')



# Get weather API key

client = secretmanager.SecretManagerServiceClient()
version_name = f'projects/parks-414615/secrets/visual_crossing_api_key/versions/most_recent'
response = client.access_secret_version(request={"name": version_name})
api_key = response.payload.data.decode("UTF-8")



# Query API for each unique city and insert to corresponding parks
# One request returns the period from 2 days ago to tomorrow

cities = set(today_data['city'])

alerts_list = []

for city in cities:
    url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/last1days/next1days?unitGroup=us&elements=datetime%2Cname%2Ctempmax%2Cfeelslikemax%2Chumidity%2Cprecip%2Cprecipprob%2Cpreciptype%2Csnowdepth%2Cwindspeed%2Ccloudcover%2Cuvindex%2Csunrise%2Csunset%2Cdescription%2Cicon&include=days%2Calerts%2Cfcst%2Cstatsfcst%2Cobs%2Cremote%2Cstats&key={api_key}&contentType=json'

    retries = 3
    for i in range(retries):
        try:
            response = requests.get(url, timeout=3.1)
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

    two_days_ago = data['days'][0]
    yesterday = data['days'][1]
    today = data['days'][2]
    tomorrow = data['days'][3]

    alerts = data['alerts']

    # List of park_ids matching this city; may be multiple
    parks_in_city = list(today_data.query('city == @city').index)

    for p in parks_in_city:
        #Insert to today_data

        today_data.loc[p, 'precip_2_days_ago_in'] = round(two_days_ago['precip'], 2)
        today_data.loc[p, 'precip_yesterday_in'] = round(yesterday['precip'], 2)
        today_data.loc[p, 'temp_max_f'] = int(round(today['tempmax']))
        today_data.loc[p, 'feels_like_max_f'] = int(round(today['feelslikemax']))
        today_data.loc[p, 'precip_prob'] = int(round(today['precipprob']))
        today_data.loc[p, 'precip_depth_in'] = round(today['precip'], 2)
        today_data.loc[p, 'cloud_cover_percent'] = int(round(today['cloudcover']))
        today_data.loc[p, 'max_wind_mph'] = int(round(today['windspeed']))
        today_data.loc[p, 'weather_description'] = today['description']
        today_data.loc[p, 'uv_index'] = int(today['uvindex'])
        today_data.loc[p, 'humidity'] = int(round(today['humidity']))
        today_data.loc[p, 'sunrise'] = today['sunrise'][:5]
        today_data.loc[p, 'sunset'] = today['sunset'][:5]
        # Icon may be: snow, rain, fog, wind, cloudy, partly-cloudy-day, partly-cloudy-night, clear-day, clear-night. Strip -day or -night if needed.
        if '-' in today['icon']: 
            icon_mod = today['icon']
            icon_mod = icon_mod.split('-')[:-1]
            icon_mod = '-'.join(icon_mod)
            today_data.loc[p, 'weather_icon'] = icon_mod
        else:
            today_data.loc[p, 'weather_icon'] = today['icon']
        if today['preciptype']:
            precip_list = today['preciptype']
            precip_list[0] = precip_list[0].title()
            today_data.loc[p, 'precip_type'] = ', '.join(precip_list)
        else:
            today_data.loc[p, 'precip_type'] = 'No precipitation'
        if alerts:
            today_data.loc[p, 'weather_alerts'] = True
            for a in alerts:
                # Also append the alert to alerts_list. These will be contents of weather_alerts table.
                alert_text = a['description']
                alerts_list.append({'date': today_data.at[1, 'date'], 'park_id': p, 'alert': alert_text})
        else:
            today_data.loc[p, 'weather_alerts'] = False
        if today['snowdepth']:
            today_data.loc[p, 'snowpack_depth_in'] = round(today['snowdepth'], 1)
        else:
            today_data.loc[p, 'snowpack_depth_in'] = 0
        
        # Insert to tomorrow_data

        tomorrow_data.loc[p, 'precip_2_days_ago_in'] = round(yesterday['precip'], 2)
        tomorrow_data.loc[p, 'precip_yesterday_in'] = round(today['precip'], 2)
        tomorrow_data.loc[p, 'temp_max_f'] = int(round(tomorrow['tempmax']))
        tomorrow_data.loc[p, 'feels_like_max_f'] = int(round(tomorrow['feelslikemax']))
        tomorrow_data.loc[p, 'precip_prob'] = int(round(tomorrow['precipprob']))
        tomorrow_data.loc[p, 'precip_depth_in'] = round(tomorrow['precip'], 2)
        tomorrow_data.loc[p, 'cloud_cover_percent'] = int(round(tomorrow['cloudcover']))
        tomorrow_data.loc[p, 'max_wind_mph'] = int(round(tomorrow['windspeed']))
        tomorrow_data.loc[p, 'weather_description'] = tomorrow['description']
        tomorrow_data.loc[p, 'uv_index'] = int(tomorrow['uvindex'])
        tomorrow_data.loc[p, 'humidity'] = int(round(tomorrow['humidity']))
        # Icon may be: snow, rain, fog, wind, cloudy, partly-cloudy-day, partly-cloudy-night, clear-day, clear-night. Strip -day or -night if needed.
        if '-' in tomorrow['icon']: 
            icon_mod = tomorrow['icon']
            icon_mod = icon_mod.split('-')[:-1]
            icon_mod = '-'.join(icon_mod)
            tomorrow_data.loc[p, 'weather_icon'] = icon_mod
        else:
            tomorrow_data.loc[p, 'weather_icon'] = tomorrow['icon']
        if tomorrow['preciptype']:
            precip_list = tomorrow['preciptype']
            precip_list[0] = precip_list[0].title()
            tomorrow_data.loc[p, 'precip_type'] = ', '.join(precip_list)
        else:
            tomorrow_data.loc[p, 'precip_type'] = 'No precipitation'
        if tomorrow['snowdepth']:
            tomorrow_data.loc[p, 'snowpack_depth_in'] = round(tomorrow['snowdepth'], 1)
        else:
            tomorrow_data.loc[p, 'snowpack_depth_in'] = 0



# Persist the data
            
if alerts_list:
    with open('weather_alerts.csv', 'w', newline='') as file:
        fieldnames = ['date', 'park_id', 'alert']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(alerts_list)

today_data.to_pickle('today_data.pickle')
tomorrow_data.to_pickle('tomorrow_data.pickle')