import os

# connect to bq

# write to weather_alerts if needed

# write to daily_conditions

# write to daily_conditions_tomorrow

# clean up .pickle and .csv files
os.remove('today_data.pickle')
os.remove('tomorrow_data.pickle')

os.remove('today_data.csv')
os.remove('tomorrow_data.csv')

# weather_alerts.csv may not exist
try:
    os.remove('weather_alerts.csv')
except FileNotFoundError:
    pass