with open('log.txt', 'a') as log:
    log.write('Now in write_to_db.py. Importing modules')

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPICallError
    import os
    import time
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

# Translate CSVs to table names

file_to_table = {'today_data.csv': 'daily_conditions', 'tomorrow_data.csv': 'daily_conditions_tomorrow', 'weather_alerts.csv': 'weather_alerts'}

with open('log.txt', 'a') as log:
    log.write('Set up BQ client')
try:
    # BigQuery client setup

    project_id = 'parks-414615'
    client = bigquery.Client(project=project_id)
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))


# Insert to each table 

for file in file_to_table.keys():
    if os.path.isfile(file): # Because weather_alerts.csv may not exist
        with open('log.txt', 'a') as log:
            log.write(f'Write file {file} to BQ')
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
                with open('log.txt', 'a') as log:
                    log.write(f'Wrote file {file}')
                break
            except (GoogleAPICallError, TimeoutError) as e:
                with open('log.txt', 'a') as log:
                    log.write(str(type(e)) + str(e) + f' on try {i} for file {file}')
                time.sleep(i + 1)
                continue
            except Exception as e:
                with open('log.txt', 'a') as log:
                    log.write(str(type(e)) + str(e) + f' on try {i} for file {file}')

with open('log.txt', 'a') as log:
    log.write('Remove pickle files')
try:
    # Clean up .pickle and .csv files

    os.remove('today_data.pickle')
    os.remove('tomorrow_data.pickle')

    os.remove('today_data.csv')
    os.remove('tomorrow_data.csv')
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

try:
    os.remove('weather_alerts.csv')
except FileNotFoundError:
    pass

with open('log.txt', 'a') as log:
    log.write('Exiting write_to_db.py')