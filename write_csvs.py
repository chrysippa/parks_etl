import pandas as pd

# Import persisted data

today_data = pd.read_pickle('today_data.pickle')

tomorrow_data = pd.read_pickle('tomorrow_data.pickle')




# Write today's data

today_data.to_csv('today_data.csv', index=False)




# Write tomorrow's data

tomorrow_data.to_csv('tomorrow_data.csv', index=False)