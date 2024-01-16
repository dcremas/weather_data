DROP TABLE obs_baro_impact;

CREATE TABLE obs_baro_impact AS
SELECT
	obs.station,
	loc.station_name,
	reg.region,
    reg.sub_region,
    loc.state,
	obs.date,
    EXTRACT(YEAR from obs.date) AS rdg_year,
	EXTRACT(MONTH from obs.date) AS rdg_month,
	EXTRACT(DAY from obs.date) AS rdg_day,
	EXTRACT(HOUR from obs.date) AS rdg_hour,
	ROUND(obs.slp::numeric - LAG(obs.slp, 3) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_3hr_diff,
	ROUND(obs.slp::numeric - LAG(obs.slp, 6) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_6hr_diff,
	ROUND(obs.slp::numeric - LAG(obs.slp, 24) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_24hr_diff
FROM observations obs
JOIN locations loc
    ON obs.station = loc.station
JOIN regions reg
    ON loc.state = reg.state
WHERE EXTRACT(YEAR from obs.date) BETWEEN 2023 AND 2023
    AND obs.source IN ('6', '7')
	AND obs.report_type IN ('FM-15')
	AND obs.slp BETWEEN 20.00 AND 35.00
	AND obs.prp <= 10.00;
