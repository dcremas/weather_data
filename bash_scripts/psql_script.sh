#!/bin/zsh

psql -t -d weatherdata -U dustincremascoli -c "SELECT DISTINCT loc.station FROM locations loc JOIN observations obs ON loc.station = obs.station" -o stations

LOCATION='/Users/dustincremascoli/PycharmProjects/output_files/by_station/'
STATIONS=($(cat stations))

for station in $STATIONS
do
  TEMP_LOC="${LOCATION}${station}.csv"
  psql -X -v st="${station}" -v loc="${TEMP_LOC}" -t -A -f /Users/dustincremascoli/PycharmProjects/WeatherData/sql/analytics_slp_decrease.sql
done

exit 0