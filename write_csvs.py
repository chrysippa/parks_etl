with open('log.txt', 'a') as log:
    log.write('Now in write_csvs.py. Importing modules')

try:
    import pandas as pd
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Unpickle data')
try:
    # Import persisted data

    today_data = pd.read_pickle('today_data.pickle')
    tomorrow_data = pd.read_pickle('tomorrow_data.pickle')
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Convert index to col')
try:
    # Change park_index from DataFrame index to column

    today_data.reset_index(inplace=True)
    today_data.rename(columns={'index':'park_id'}, inplace=True)

    tomorrow_data.reset_index(inplace=True)
    tomorrow_data.rename(columns={'index':'park_id'}, inplace=True)
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Move date to 1st col')
try:
    # Move date column to beginning; BigQuery expects this ordering

    date_today = today_data.pop('date')
    today_data.insert(0, 'date', date_today)

    date_tomorrow = tomorrow_data.pop('date')
    tomorrow_data.insert(0, 'date', date_tomorrow)
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Remove metadata')
try:
    # Remove metadata

    today_data.drop(columns=['name', 'type', 'city', 'site_url'], inplace=True)
    tomorrow_data.drop(columns=['name', 'type', 'city', 'site_url'], inplace=True)
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Write csvs')
try:
    # Write CSVs

    today_data.to_csv('today_data.csv', index=False)
    tomorrow_data.to_csv('tomorrow_data.csv', index=False)
except Exception as e:
    with open('log.txt', 'a') as log:
        log.write(str(type(e)) + str(e))

with open('log.txt', 'a') as log:
    log.write('Exiting write_csvs.py')