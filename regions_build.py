import csv
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import create_engine, insert
from sqlalchemy import Column, Integer, String
from sqlalchemy.sql import text
import shared_funcs

Base = declarative_base()


class Regions(Base):

    __tablename__ = "regions"

    id = Column(Integer, primary_key=True)
    state = Column(String)
    region = Column(String)
    sub_region = Column(String)


url = shared_funcs.database_path()
engine = create_engine(url=url, pool_size=5, pool_recycle=3600)
Base.metadata.create_all(engine)

with open('metadata/regions.csv', 'r', newline='') as read_file:
    reader = csv.reader(read_file, delimiter=',')
    region_data = (record for record in reader)

    region_data_clean = list()

    for item in region_data:
        temp_dict = dict()
        temp_dict["state"] = item[0]
        temp_dict["region"] = item[1]
        temp_dict["sub_region"] = item[2]
        region_data_clean.append(temp_dict)

delete_query = f"DELETE FROM regions;"

with engine.connect() as connection:
    statement = text(delete_query)
    result = connection.execute(statement)
    connection.commit()

session = Session(bind=engine)
session.execute(insert(Regions), region_data_clean)
session.commit()
