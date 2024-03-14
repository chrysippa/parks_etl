import pickle
import csv

with open('data.pickle', 'rb') as file:
    data = pickle.load(file)

# convert all data to string
for park in data:
    for key in park.keys():
        park[key] = str(park[key])

fieldnames = data[0].keys()

with open('data.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(data)