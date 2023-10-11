DROP TABLE loc_subset;

CREATE TABLE loc_subset AS
SELECT DISTINCT
	(SELECT DISTINCT EXTRACT(YEAR from obs.date)) AS year,
	loc.station,
	loc.station_name,
	reg.region,
    reg.sub_region,
    loc.state,
    loc.lat,
    loc.lon
FROM locations loc
JOIN observations obs
    ON loc.station = obs.station
JOIN regions reg
    ON loc.state = reg.state;