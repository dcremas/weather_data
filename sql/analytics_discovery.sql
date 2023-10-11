WITH data_transform_prelim AS (
	SELECT
		obs.station,
		obs.date,
		EXTRACT(YEAR from obs.date) AS rdg_year,
		EXTRACT(MONTH from obs.date) AS rdg_month,
		EXTRACT(DAY from obs.date) AS rdg_day,
		obs.tmp,
		obs.prp,
		obs.slp,
		ROUND(obs.slp::numeric - LAG(obs.slp, 3) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_3hr_diff,
		ROUND(obs.slp::numeric - LAG(obs.slp, 6) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_6hr_diff,
		ROUND(obs.slp::numeric - LAG(obs.slp, 24) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_24hr_diff
	FROM observations obs
	WHERE EXTRACT(YEAR from date) BETWEEN 2022 AND 2022
		AND obs.source IN ('6', '7')
		AND obs.report_type IN ('FM-15')
		AND obs.slp BETWEEN 20.00 AND 35.00
		AND obs.prp <= 10.00
)

SELECT
	dtp.station,
	loc.station_name,
	dtp.rdg_year,
	MIN(dtp.slp_3hr_diff) as slp_3hr_min,
	MIN(dtp.slp_6hr_diff) as slp_6hr_min,
	MIN(dtp.slp_24hr_diff) as slp_24hr_min,
	MAX(dtp.slp_3hr_diff) as slp_3hr_max,
	MAX(dtp.slp_6hr_diff) as slp_6hr_max,
	MAX(dtp.slp_24hr_diff) as slp_24hr_max
FROM data_transform_prelim dtp
JOIN locations loc
    ON dtp.station = loc.station
GROUP BY dtp.station, loc.station_name, rdg_year;
