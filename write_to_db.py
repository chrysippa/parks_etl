from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
import os
import time


# Translate CSVs to table names

file_to_table = {'today_data.csv': 'daily_conditions', 'tomorrow_data.csv': 'daily_conditions_tomorrow', 'weather_alerts.csv': 'weather_alerts'}



# BigQuery client setup

project_id = 'parks-414615'
client = bigquery.Client(project=project_id)



# Insert to each table 

for file in file_to_table.keys():
    if os.path.isfile(file): # Because weather_alerts.csv may not exist
        table_id = f'{project_id}.parks.{file_to_table[file]}'

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, 
                                            skip_leading_rows=1, 
                                            autodetect=False, 
                                            allow_jagged_rows=True)

        retries = 3
        for i in range(retries):
            try:
                with open(file, 'rb') as source_file:
                    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                job.result()
                break
            except (GoogleAPICallError, TimeoutError):
                time.sleep(i + 1)
                continue


# Clean up .pickle and .csv files

os.remove('today_data.pickle')
os.remove('tomorrow_data.pickle')

os.remove('today_data.csv')
os.remove('tomorrow_data.csv')

try:
    os.remove('weather_alerts.csv')
except FileNotFoundError:
    pass