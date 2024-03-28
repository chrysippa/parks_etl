import pandas as pd
import requests
from visual_crossing_key import visual_crossing_api_key

# Import persisted data

today_data = pd.read_pickle('today_data.pickle')

tomorrow_data = pd.read_pickle('tomorrow_data.pickle')

park_metadata = pd.read_pickle('park_metadata.pickle')




# get park ids and city, state

# Test case - Afton State Park
city = "Hastings, MN"

url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/last1days/next1days?unitGroup=us&elements=datetime%2Cname%2Ctempmax%2Cfeelslikemax%2Chumidity%2Cprecip%2Cprecipprob%2Cpreciptype%2Csnowdepth%2Cwindspeed%2Ccloudcover%2Cuvindex%2Csunrise%2Csunset%2Cdescription%2Cicon&include=days%2Calerts%2Cfcst%2Cstatsfcst%2Cobs%2Cremote%2Cstats&key={visual_crossing_api_key}&contentType=json"

# Handle these status codes: 
# 400 BAD_REQUEST The format of the API is incorrect or an invalid parameter or combination of parameters was supplied
# 401 UNAUTHORIZED There is a problem with the API key, account or subscription. May also be returned if a feature is requested for which the account does not have access to.
# 404 NOT_FOUND The request cannot be matched to any valid API request endpoint structure.
# 429 TOO_MANY_REQUESTS The account has exceeded their assigned limits. 
# 500 INTERNAL_SERVER_ERROR A general error has occurred processing the request.

response = requests.get(url) # add timeout=num_seconds and try-except block in case of Timeout exception

status = response.status_code
data = response.json()

print(status)
print(data)

two_days_ago = data["days"][0]
yesterday = data["days"][1]
today = data["days"][2]
tomorrow = data["days"][3]
alerts = data["alerts"]

if not alerts:
    alerts = None
else:
    pass # handle 1+ alerts in list

precip_yesterday_in = yesterday["precip"]
precip_2_days_ago_in = two_days_ago["precip"]




# Persist the data

today_data.to_pickle('today_data.pickle')

tomorrow_data.to_pickle('tomorrow_data.pickle')