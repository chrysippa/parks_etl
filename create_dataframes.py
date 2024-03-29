from google.cloud.bigquery_storage import BigQueryReadClient, types
import pandas as pd




# Fetch from database: park metadata needed in later ETL steps
# Also fetch from special_days to reuse connection

# to authenticate in production, see here: https://cloud.google.com/docs/authentication/provide-credentials-adc#attached-sa

project_id = 'parks-414615'

client = BigQueryReadClient()

table = f'projects/{project_id}/datasets/parks/tables/parks'

requested_session = types.ReadSession()
requested_session.table = table
requested_session.data_format = types.DataFormat.AVRO # or can change to Apache Arrow; maybe good for date support

requested_session.read_options.selected_fields = ['park_id', 'name', 'type', 'city', 'site_url'] # columns to pull

parent = f'projects/{project_id}'
session = client.create_read_session(
    parent=parent,
    read_session=requested_session,
    max_stream_count=1
)
reader = client.read_rows(session.streams[0].name)

rows = reader.rows(session)

pulled_data_parks = []

try:
    for row in rows:
        pulled_data_parks.append(row) # append 1 dict for row
except EOFError:
    pass

# pull from special_days: date, park_id, holiday, note (aka all columns)




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
    park_id = park['park_id']
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
    park_id = park['park_id']
    row = {field: (int(park_id) if field=='park_id' else None) for field in tomorrow_fieldnames}
    tomorrow_data_rows.append(row)

# Create dataframe from rows
tomorrow_data = pd.DataFrame(data=tomorrow_data_rows)




# Create dataframe for park metadata
park_metadata = pd.DataFrame(data=pulled_data_parks)




# Append metadata to other dataframes
today_data = pd.merge(today_data, park_metadata, on='park_id')
tomorrow_data = pd.merge(tomorrow_data, park_metadata, on='park_id')

# turn park_id into an index for ease of insertion
today_data.set_index('park_id', inplace=True)
tomorrow_data.set_index('park_id', inplace=True)




# Insert special_days information

# Insert today's and tomorrow's date



# Persist the data

today_data.to_pickle('today_data.pickle')

tomorrow_data.to_pickle('tomorrow_data.pickle')

park_metadata.to_pickle('park_metadata.pickle')

"""
print(today_data)
print(tomorrow_data)
print(park_metadata)
"""