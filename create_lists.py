import pickle

# create connection
# pull from parks: park_id, name, type, city, accuweather_location
# pull from special_days: date, park_id, holiday, note (aka all columns)

# Create empty list for today's data

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

today_data_row = {field: None for field in today_fieldnames}

# Test case: 4 parks
today_data = [today_data_row for i in [0, 1, 2, 3]]




# Create empty list for tomorrow's data

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

tomorrow_data_row = {field: None for field in tomorrow_fieldnames}

# Test case: 4 parks
tomorrow_data = [tomorrow_data_row for i in [0, 1, 2, 3]]




# Create empty list for park metadata - needed for later ETL steps

metadata_fieldnames = ["park_id", 
                       "name",
                       "type", 
                       "city", 
                       "accuweather_location"]

park_metadata_row = {field: None for field in metadata_fieldnames}


# Test case: 4 parks
park_metadata = [park_metadata_row for i in [0, 1, 2, 3]]

# TODO: insert pulled metadata



# Persist the lists

with open('today_data.pickle', 'wb') as today_file:
    pickle.dump(today_data, today_file)

with open('tomorrow_data.pickle', 'wb') as tomorrow_file:
    pickle.dump(tomorrow_data, tomorrow_file)

with open('park_metadata.pickle', 'wb') as metadata_file:
    pickle.dump(park_metadata, metadata_file)