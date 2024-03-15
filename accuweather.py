import requests
import pickle
from accuweather_key import accuweather_api_key

with open('today_data.pickle', 'rb') as today_file:
    today_data = pickle.load(today_file)

with open('tomorrow_data.pickle', 'rb') as tomorrow_file:
    tomorrow_data = pickle.load(tomorrow_file)

# get park ids and accuweather location key codes

# use gzip encoding by adding an HTTP header
# for each park
# try:
#   location_key = 
#   park_url = f"http://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?apikey={accuweather_api_key}&details=true"
#   response = requests.get(park_url)
# except HTTP response 500:
#   try again
# except HTTP response not 400 or 500:
#   stuff

# Test case - Afton State Park
location_key = str(2228246)
park_url = f"http://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}?apikey={accuweather_api_key}&details=true"

response = requests.get(park_url) # add timeout=num_seconds and use try-except block in case of Timeout exception

status = response.status_code

# Response structure:
""" {"DailyForecasts":[ 
                        {"AirAndPollen": [ 
                                            {"Name": "AirQuality or pollen type", "Category": "Textcategory", "Type": "AQtype"} 
                                        ]},
                        {"AirAndPollen tomorrow": [same structure] } 
                ]
} """

data = response.json()

today = data["DailyForecasts"][0]
tomorrow = data["DailyForecasts"][1]

today_air = today["AirAndPollen"]
tomorrow_air = tomorrow["AirAndPollen"]

for datum in today_air:
    if datum["Name"] == "AirQuality":
        today_aq_category = datum["Category"]
        today_aq_type = datum["Type"]
    if datum["Name"] == "Grass":
        today_grass = datum["Category"]
    if datum["Name"] == "Mold":
        today_mold = datum["Category"]
    if datum["Name"] == "Ragweed":
        today_ragweed = datum["Category"]
    if datum["Name"] == "Tree":
        today_tree = datum["Category"]


print("Today aq category: ", today_aq_category)
print("Today aq type: ", today_aq_type)
print("Today grass: ", today_grass)
print("Today mold: ", today_mold)
print("Today ragweed: ", today_ragweed)
print("Today tree: ", today_tree)

with open('today_data.pickle', 'wb') as today_file:
    pickle.dump(today_data, today_file)

with open('tomorrow_data.pickle', 'wb') as tomorrow_file:
    pickle.dump(tomorrow_data, tomorrow_file)