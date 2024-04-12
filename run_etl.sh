#! /bin/bash

echo "$(date) Entering create_dataframes.py" >> log.txt
python3 create_dataframes.py

echo "$(date) Entering visual_crossing.py" >> log.txt
python3 visual_crossing.py

echo "$(date) Entering accuweather.py" >> log.txt
python3 accuweather.py

echo "$(date) Entering write_csvs.py" >> log.txt
python3 write_csvs.py

echo "$(date) Entering write_to_db.py" >> log.txt
python3 write_to_db.py