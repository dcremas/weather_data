import os
import csv
from datetime import datetime
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from database_ddl import Observations
import shared_funcs

files = [file.split('.')[0] for file in os.listdir('yearly_files/2023') if file.split('.')[1] == 'csv']

delete_query = """
DELETE FROM observations
WHERE EXTRACT(YEAR from date) = 2023;
"""

url = shared_funcs.database_path()
engine = create_engine(url=url)

with engine.connect() as connection:
    statement = text(delete_query)
    result = connection.execute(statement)
    connection.commit()

data_clean = list()

for current_id in files:
    path = f"yearly_files/2023/{current_id}.csv"

    with open(path, 'r', newline='') as read_file:
        reader = csv.reader(read_file, delimiter=',')
        data = (record for idx, record in enumerate(reader) if idx > 0)

        for item in data:
            temp_dict = dict()
            temp_dict["station"] = item[1]
            temp_dict["date"] = datetime.strptime(item[2], '%Y-%m-%dT%H:%M:%S')
            temp_dict["source"] = item[3]
            temp_dict["report_type"] = item[4]
            temp_dict["wnd"] = shared_funcs.mps_to_mph(item[5].split(',')[3])
            temp_dict["cig"] = shared_funcs.meters_to_miles(item[6].split(',')[0])
            temp_dict["vis"] = shared_funcs.meters_to_miles(item[7].split(',')[0])
            temp_dict["tmp"] = shared_funcs.celsius_to_fahrenheit(item[8].split(',')[0])
            temp_dict["dew"] = shared_funcs.celsius_to_fahrenheit(item[9].split(',')[0])
            temp_dict["slp"] = shared_funcs.millibar_to_hg(item[10].split(',')[0])
            try:
                temp_dict["prp"] = shared_funcs.millimeters_to_inches(item[11].split(',')[1])
            except IndexError:
                temp_dict["prp"] = 0.0

            if temp_dict["report_type"] in ['FM-12', 'FM-15']:
                data_clean.append(temp_dict)

session = Session(bind=engine)
session.execute(insert(Observations), data_clean)
session.commit()
