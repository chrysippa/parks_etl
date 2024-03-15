import pickle
import requests

with open('today_data.pickle', 'rb') as today_file:
    today_data = pickle.load(today_file)

with open('tomorrow_data.pickle', 'rb') as tomorrow_file:
    tomorrow_data = pickle.load(tomorrow_file)

url = 

response = requests.get(url) # add timeout=num_seconds and try-except block to handle Timeout exception




with open('today_data.pickle', 'wb') as today_file:
    pickle.dump(today_data, today_file)

with open('tomorrow_data.pickle', 'wb') as tomorrow_file:
    pickle.dump(tomorrow_data, tomorrow_file)