#/bin/zsh

# A place to run postgres sequel scripts from commands.

time_start=$(date +%s)

psql -d weatherdata -U dustincremascoli -f sql/analytics_slp_decrease.sql -o output_files/analytics.csv

time_stop=$(date +%s)

diff=$(( time_stop - time_start ))

echo "The script took $diff seconds to complete."

exit 0
