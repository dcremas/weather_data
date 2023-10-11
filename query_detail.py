from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
import shared_funcs

year, month, station = 2023, 10, '72530094846'

select_query = f'''
SELECT
	obs.station,
	loc.station_name,
	loc.state,
	obs.date,
	EXTRACT(YEAR from obs.date) AS rdg_year,
	EXTRACT(MONTH from obs.date) AS rdg_month,
	EXTRACT(DAY from obs.date) AS rdg_day,
	obs.tmp,
	obs.prp,
	obs.dew,
	obs.slp,
	ROUND(obs.slp::numeric - LAG(obs.slp, 3) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_3hr_diff
FROM observations obs
JOIN locations loc
	ON obs.station = loc.station
WHERE obs.station = '{station}'
    AND EXTRACT(YEAR from obs.date) = {year}
    AND EXTRACT(MONTH from obs.date) = {month}
    AND obs.source IN ('6', '7')
	AND obs.report_type IN ('FM-15')
	AND obs.slp BETWEEN 20.00 AND 35.00
	AND obs.prp <= 10.00;
'''

url = shared_funcs.database_path()
engine = create_engine(url=url)

statement = text(select_query)
df = pd.read_sql(statement, engine.connect())

print(len(df))
print(df.to_markdown())
