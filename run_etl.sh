#! /bin/bash

python3 create_dataframes.py
python3 visual_crossing.py
python3 accuweather.py
python3 write_csvs.py
python3 write_to_db.py