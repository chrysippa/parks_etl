with open('log.txt', 'a') as log:
    log.write('Now in accuweather.py. Importing modules')

try:
    import pandas as pd
    import requests
    from google.cloud import secretmanager
    import time
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Unpickle dfs')
try:
    # Import persisted data

    today_data = pd.read_pickle('today_data.pickle')
    tomorrow_data = pd.read_pickle('tomorrow_data.pickle')
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Get API key from Secrets')
try:
    # Get API key

    client = secretmanager.SecretManagerServiceClient()
    version_name = f'projects/parks-414615/secrets/accuweather_api_key/versions/most_recent'
    response = client.access_secret_version(request={"name": version_name})
    api_key = response.payload.data.decode("UTF-8")
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))


# Query API for location codes for each unique city

cities = set(today_data['city'])

for city in cities:
    with open('log.txt', 'a') as log:
        log.write(f'Query for city {city}')
    location_url = f'http://dataservice.accuweather.com/locations/v1/cities/US/search?apikey={api_key}&q={city}'
    headers = {'Accept-Encoding': 'gzip'}

    retries = 3
    for i in range(retries):
        try:
            response = requests.get(location_url, headers=headers, timeout=3.1)
            response.raise_for_status()
            with open('log.txt', 'a') as log:
                log.write(f'Got loc response for city {city}')
            break
        except (requests.Timeout, requests.ConnectionError) as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e) + f' on try {i}')
            time.sleep(i + 1)
            continue
        except requests.HTTPError as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e) + f' on try {i} w/ code {response.status_code}')
            if response.status_code == 500:
                time.sleep(i + 1)
                continue
        except Exception as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e) + f' on try {i}')

    with open('log.txt', 'a') as log:
        log.write('Get JSON & loc key from response')
    try:
        location_data = response.json()
        location_key = location_data[0]['Key']
    except Exception as e:
        with open('log.txt', 'a') as log:
            log.write(str(type(e)) + str(e))


    # Query API for air quality per location

    url = f'http://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?apikey={api_key}&details=true'

    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=3.1)
            response.raise_for_status()
            with open('log.txt', 'a') as log:
                log.write(f'Got data for city {city}')
            break
        except (requests.Timeout, requests.ConnectionError) as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e) + f' on try {i}')
            time.sleep(i + 1)
            continue
        except requests.HTTPError as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e) + f' on try {i} w/ code {response.status_code}')
            if response.status_code == 500:
                time.sleep(i + 1)
                continue
        except Exception as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e) + f' on try {i}')

    with open('log.txt', 'a') as log:
        log.write('Get JSON & data from response')
    try:
        data = response.json()
        today = data['DailyForecasts'][0]['AirAndPollen']
        tomorrow = data['DailyForecasts'][1]['AirAndPollen']
    except Exception as e:
        with open('log.txt', 'a') as log:
            log.write(str(type(e)) + str(e))

    with open('log.txt', 'a') as log:
        log.write(f'Get parks in city {city}')
    try:
        # List of park_ids matching this city; may be multiple
        parks_in_city = list(today_data.query('city == @city').index)
    except Exception as e:
        with open('log.txt', 'a') as log:
            log.write(str(type(e)) + str(e))

    for p in parks_in_city:
        #Insert to today_data
        with open('log.txt', 'a') as log:
            log.write('Insert to today')
        try:
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
        except Exception as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e))

        with open('log.txt', 'a') as log:
            log.write('Insert to tomorrow')
        try:
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
        except Exception as e:
            with open('log.txt', 'a') as log:
                log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Pickle data')
try:
    # Persist the data

    today_data.to_pickle('today_data.pickle')
    tomorrow_data.to_pickle('tomorrow_data.pickle')
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Exiting accuweather.py')