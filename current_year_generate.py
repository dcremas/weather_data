import os
import pandas as pd

select_cols = [
    'STATION', 'DATE', 'SOURCE', 'REPORT_TYPE',
    'WND', 'CIG', 'VIS', 'TMP', 'DEW', 'SLP', 'AA1'
]

path = f"https://www.ncei.noaa.gov/data/global-hourly/access/2023/"

files = [file.split('.')[0] for file in os.listdir('yearly_files/2023') if file.split('.')[1] == 'csv']

for current_id in files:
    file_ftp = path + current_id + '.csv'
    df = pd.read_csv(file_ftp, usecols=select_cols, low_memory=False)
    file_local = 'yearly_files/2023/' + current_id + '.csv'
    df.to_csv(file_local)
