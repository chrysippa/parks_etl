import pandas as pd

# Import persisted data

today_data = pd.read_pickle('today_data.pickle')
tomorrow_data = pd.read_pickle('tomorrow_data.pickle')



# Change park_index from DataFrame index to column

today_data.reset_index(inplace=True)
today_data.rename(columns={'index':'park_id'}, inplace=True)

tomorrow_data.reset_index(inplace=True)
tomorrow_data.rename(columns={'index':'park_id'}, inplace=True)



# Move date column to beginning; BigQuery expects this ordering

date_today = today_data.pop('date')
today_data.insert(0, 'date', date_today)

date_tomorrow = tomorrow_data.pop('date')
tomorrow_data.insert(0, 'date', date_tomorrow)



# Remove metadata

today_data.drop(columns=['name', 'type', 'city', 'site_url'], inplace=True)
tomorrow_data.drop(columns=['name', 'type', 'city', 'site_url'], inplace=True)



# Write CSVs

today_data.to_csv('today_data.csv', index=False)
tomorrow_data.to_csv('tomorrow_data.csv', index=False)