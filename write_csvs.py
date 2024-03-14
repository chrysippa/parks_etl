import pickle
import csv

# Write today's data

with open('today_data.pickle', 'rb') as today_file:
    today_data = pickle.load(today_file)

# convert all data to string
for park in today_data:
    for key in park.keys():
        park[key] = str(park[key])

today_fieldnames = today_data[0].keys()

with open('today_data.csv', 'w', newline='') as today_csv:
    writer = csv.DictWriter(today_csv, fieldnames=today_fieldnames)

    writer.writeheader()
    writer.writerows(today_data)



# Write tomorrow's data

with open('tomorrow_data.pickle', 'rb') as tomorrow_file:
    tomorrow_data = pickle.load(tomorrow_file)

# convert all data to string
for park in tomorrow_data:
    for key in park.keys():
        park[key] = str(park[key])

tomorrow_fieldnames = tomorrow_data[0].keys()

with open('tomorrow_data.csv', 'w', newline='') as tomorrow_csv:
    writer = csv.DictWriter(tomorrow_csv, fieldnames=tomorrow_fieldnames)

    writer.writeheader()
    writer.writerows(tomorrow_data)