import pandas as pd

# create connection
# pull from parks: park_id, name, type, city, accuweather_location
# pull from special_days: date, park_id, holiday, note (aka all columns)

# Test case: 4 parks
pulled_data_parks = [
    ['123', 'Afton State Park', 'state', 'Afton, MN', '24234'],
    ['456', 'Banning State Park', 'state', 'Nowhere, MN', '32535'],
    ['789', 'Jay Cooke State Park', 'state', 'Anytown, MN', '65465'],
    ['987', 'Lake Maria State Park', 'state', 'Village, MN', '86733']
]




# Create empty dataframe for today's data

today_fieldnames = ['date', 
                'park_id', 
                'temp_max_f', 
                'feels_like_max_f', 
                'precip_prob', 
                'precip_depth_in', 
                'precip_type', 
                'cloud_cover_percent', 
                'air_quality_type', 
                'air_quality_level', 
                'max_wind_mph', 
                'weather_description', 
                'weather_icon', 
                'weather_alerts', 
                'precip_yesterday_in', 
                'precip_2_days_ago_in', 
                'park_alerts', 
                'uv_index', 
                'humidity', 
                'snowpack_depth_in', 
                'pollen_mold', 
                'pollen_tree', 
                'pollen_ragweed', 
                'pollen_grass', 
                'fall_colors', 
                'holiday', 
                'special_park_day', 
                'sunrise', 
                'sunset',
                'special_day_note']

today_data_rows = []

# Create 1 row per park. Filled with park_id and value 'None' for other columns.
for park in pulled_data_parks:
    park_id = park[0]
    row = {field: (int(park_id) if field=='park_id' else None) for field in today_fieldnames}
    today_data_rows.append(row)

# Create dataframe from rows
today_data = pd.DataFrame(data=today_data_rows)




# Create empty dataframe for tomorrow's data

tomorrow_fieldnames = ['date', 
                'park_id', 
                'temp_max_f', 
                'feels_like_max_f', 
                'precip_prob', 
                'precip_depth_in', 
                'precip_type', 
                'cloud_cover_percent', 
                'air_quality_type', 
                'air_quality_level', 
                'max_wind_mph', 
                'weather_description', 
                'weather_icon',  
                'precip_yesterday_in', 
                'precip_2_days_ago_in', 
                'uv_index', 
                'humidity', 
                'snowpack_depth_in', 
                'pollen_mold', 
                'pollen_tree', 
                'pollen_ragweed', 
                'pollen_grass', 
                'holiday', 
                'special_park_day',
                'special_day_note']

tomorrow_data_rows = []

# Create 1 row per park. Filled with park_id and value 'None' for other columns.
for park in pulled_data_parks:
    park_id = park[0]
    row = {field: (int(park_id) if field=='park_id' else None) for field in tomorrow_fieldnames}
    tomorrow_data_rows.append(row)

# Create dataframe from rows
tomorrow_data = pd.DataFrame(data=tomorrow_data_rows)




# Create empty dataframe for park metadata - needed for later ETL steps

metadata_fieldnames = ['park_id', 
                       'name',
                       'type', 
                       'city', 
                       'accuweather_location']

park_metadata_rows = []

# Create 1 row per park. Filled with park_id and value 'None' for other columns.
for park in pulled_data_parks:
    park_id = park[0]
    row = {field: (int(park_id) if field=='park_id' else None) for field in metadata_fieldnames}
    park_metadata_rows.append(row)

# Create dataframe from rows
park_metadata = pd.DataFrame(data=park_metadata_rows)



# TODO: insert pulled metadata



# Persist the data

#today_data.to_pickle('today_data.pickle')

#tomorrow_data.to_pickle('tomorrow_data.pickle')

#park_metadata.to_pickle('park_metadata.pickle')

print(today_data)
print(tomorrow_data)
print(park_metadata)