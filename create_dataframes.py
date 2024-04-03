from google.cloud.bigquery_storage import BigQueryReadClient, types
import datetime
import pandas as pd


# Fetch from database: park IDS and metadata needed for ETL

# to authenticate in production, see here: https://cloud.google.com/docs/authentication/provide-credentials-adc#attached-sa

project_id = 'parks-414615'

client = BigQueryReadClient()

table = f'projects/{project_id}/datasets/parks/tables/parks'

requested_session = types.ReadSession()
requested_session.table = table
requested_session.data_format = types.DataFormat.AVRO

requested_session.read_options.selected_fields = ['park_id', 
                                                  'name', 
                                                  'type', 
                                                  'city', 
                                                  'site_url']

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
        pulled_data_parks.append(row) # append rows as dicts
except EOFError:
    pass



# Also fetch from special_days table, to reuse client object as recommended

today_dt = datetime.date.today()
today = str(today_dt)
tomorrow = str(today_dt + datetime.timedelta(days=1))

table = f'projects/{project_id}/datasets/parks/tables/special_days'

requested_session = types.ReadSession()
requested_session.table = table
requested_session.data_format = types.DataFormat.AVRO 

requested_session.read_options.selected_fields = ['date', 
                                                  'park_id', 
                                                  'holiday', 
                                                  'note']
requested_session.read_options.row_restriction = f'date = "{today}" OR date ="{tomorrow}"' # WHERE clause

parent = f'projects/{project_id}'
session = client.create_read_session(
    parent=parent,
    read_session=requested_session,
    max_stream_count=1
)
reader = client.read_rows(session.streams[0].name)

rows = reader.rows(session)

pulled_data_days = []

try:
    for row in rows:
        pulled_data_days.append(row) # append rows as dicts
except EOFError:
    pass



# Create dataframe for today's data

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

# Dataframe rows contain park_id, otherwise value 'None' for other columns
for park in pulled_data_parks:
    park_id = park['park_id']
    row = {field: (int(park_id) if field=='park_id' else None) for field in today_fieldnames}
    today_data_rows.append(row)

today_data = pd.DataFrame(data=today_data_rows)



# Create dataframe for tomorrow's data

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

# Dataframe rows contain park_id, otherwise value 'None' for other columns
for park in pulled_data_parks:
    park_id = park['park_id']
    row = {field: (int(park_id) if field=='park_id' else None) for field in tomorrow_fieldnames}
    tomorrow_data_rows.append(row)

tomorrow_data = pd.DataFrame(data=tomorrow_data_rows)



# Append metadata to dataframes

park_metadata = pd.DataFrame(data=pulled_data_parks)

today_data = pd.merge(today_data, park_metadata, on='park_id')
tomorrow_data = pd.merge(tomorrow_data, park_metadata, on='park_id')



# Turn park_id into an index for ease of insertion

today_data.set_index('park_id', inplace=True)
tomorrow_data.set_index('park_id', inplace=True)



# Insert today's and tomorrow's date

num_parks = today_data.shape[0]

today_data['date'] = [today for x in range(num_parks)]
tomorrow_data['date'] = [tomorrow for x in range(num_parks)]



# Insert special_days information

if pulled_data_days:
    for day in pulled_data_days:
        park_id = day['park_id']
        holiday = bool(day['holiday'])
        day_note = day['note']
        if str(day['date']) == today:
            today_data.at[park_id, 'special_day_note'] = day_note
            if holiday:
                today_data.at[park_id, 'holiday'] = True
            else:
                today_data.at[park_id, 'special_park_day'] = True
                
        elif str(day['date']) == tomorrow:
            tomorrow_data.at[park_id, 'special_day_note'] = day_note
            if holiday:
                tomorrow_data.at[park_id, 'holiday'] = True
            else:
                tomorrow_data.at[park_id, 'special_park_day'] = True

# If not a special day or holiday, insert False

today_data['holiday'].fillna(value='False', inplace=True)
today_data['special_park_day'].fillna(value='False', inplace=True)

tomorrow_data['holiday'].fillna(value='False', inplace=True)
tomorrow_data['special_park_day'].fillna(value='False', inplace=True)



# Persist the data

today_data.to_pickle('today_data.pickle')
tomorrow_data.to_pickle('tomorrow_data.pickle')
park_metadata.to_pickle('park_metadata.pickle')