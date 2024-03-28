import pandas as pd
import requests

# Import persisted data

today_data = pd.read_pickle('today_data.pickle')

tomorrow_data = pd.read_pickle('tomorrow_data.pickle')

park_metadata = pd.read_pickle('park_metadata.pickle')




url = 

response = requests.get(url) # add timeout=num_seconds and try-except block to handle Timeout exception




# Persist the data

today_data.to_pickle('today_data.pickle')

tomorrow_data.to_pickle('tomorrow_data.pickle')