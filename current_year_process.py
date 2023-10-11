from datetime import datetime
import polars as pl
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from database_ddl import Observations
import shared_funcs

year_no = 2022

delete_query = f"DELETE FROM observations WHERE EXTRACT(YEAR from date) = {year_no};"

url = shared_funcs.database_path('weatherdata')
engine = create_engine(url=url)

with engine.connect() as connection:
    statement = text(delete_query)
    result = connection.execute(statement)
    connection.commit()

spark = (SparkSession.builder.appName("pyspark_parquet")
         .config("spark.sql.crossJoin.enabled", "true")
         .getOrCreate())

weather_data = spark.read.format("parquet").load(f"yearly_files_parquet/{year_no}/data.parquet")
weather_data.createOrReplaceGlobalTempView("weather_data")

data_clean = list()

data = (tuple(i) for i in weather_data.collect())

for item in data:
    temp_dict = dict()
    temp_dict["station"] = item[0]
    temp_dict["date"] = datetime.strptime(item[1], '%Y-%m-%dT%H:%M:%S')
    temp_dict["source"] = item[2]
    temp_dict["report_type"] = item[3]
    temp_dict["wnd"] = shared_funcs.mps_to_mph(item[4].split(',')[3])
    temp_dict["cig"] = shared_funcs.meters_to_miles(item[5].split(',')[0])
    temp_dict["vis"] = shared_funcs.meters_to_miles(item[6].split(',')[0])
    temp_dict["tmp"] = shared_funcs.celsius_to_fahrenheit(item[7].split(',')[0])
    temp_dict["dew"] = shared_funcs.celsius_to_fahrenheit(item[8].split(',')[0])
    temp_dict["slp"] = shared_funcs.millibar_to_hg(item[9].split(',')[0])
    try:
        temp_dict["prp"] = shared_funcs.millimeters_to_inches(item[10].split(',')[1])
    except (IndexError, AttributeError):
        temp_dict["prp"] = 0.0

    if temp_dict["report_type"] in ['FM-12', 'FM-15']:
        data_clean.append(temp_dict)

session = Session(bind=engine)
session.execute(insert(Observations), data_clean)
session.commit()

connection_uri = shared_funcs.connection_uri()
query = f"SELECT * FROM observations WHERE EXTRACT(YEAR from date) = {year_no}"

polars_df = pl.read_database_uri(query=query, uri=connection_uri)
polars_df.write_parquet(f"yearly_files_parquet/{year_no}/data_clean.parquet")