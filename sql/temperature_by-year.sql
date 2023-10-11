WITH data_transform_prelim_1 AS (
	SELECT
		obs.station,
		loc.station_name,
		loc.state,
		obs.date,
		EXTRACT(YEAR from obs.date) AS rdg_year,
		EXTRACT(MONTH from obs.date) AS rdg_month,
		EXTRACT(DAY from obs.date) AS rdg_day,
		obs.tmp
	FROM observations obs
	JOIN locations loc
		ON obs.station = loc.station
	WHERE loc.state = 'CA'
		AND EXTRACT(YEAR from obs.date) BETWEEN 2005 AND 2023
		AND obs.source IN ('6', '7')
		AND obs.report_type IN ('FM-15')
		AND obs.slp BETWEEN 20.00 AND 35.00
		AND obs.prp <= 10.00
		AND obs.tmp BETWEEN -150.00 AND 150.00
),

data_transform_prelim_2 AS (
	SELECT
		station,
		station_name,
		state,
		rdg_year,
		rdg_month,
		rdg_day,
		ROUND(MIN(tmp)::numeric, 2) AS tmp_min,
		ROUND(MAX(tmp)::numeric, 2) AS tmp_max
	FROM data_transform_prelim_1
	GROUP BY station, station_name, state, rdg_year, rdg_month, rdg_day
),


data_transform_final AS (
	SELECT
		rdg_month,
		station_name,
		state,
		ROUND(AVG(CASE WHEN rdg_year = 2005 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_05,
		ROUND(AVG(CASE WHEN rdg_year = 2006 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_06,
		ROUND(AVG(CASE WHEN rdg_year = 2007 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_07,
		ROUND(AVG(CASE WHEN rdg_year = 2008 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_08,
		ROUND(AVG(CASE WHEN rdg_year = 2009 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_09,
		ROUND(AVG(CASE WHEN rdg_year = 2010 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_10,
		ROUND(AVG(CASE WHEN rdg_year = 2011 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_11,
		ROUND(AVG(CASE WHEN rdg_year = 2012 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_12,
		ROUND(AVG(CASE WHEN rdg_year = 2013 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_13,
		ROUND(AVG(CASE WHEN rdg_year = 2014 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_14,
		ROUND(AVG(CASE WHEN rdg_year = 2015 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_15,
		ROUND(AVG(CASE WHEN rdg_year = 2016 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_16,
		ROUND(AVG(CASE WHEN rdg_year = 2017 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_17,
		ROUND(AVG(CASE WHEN rdg_year = 2018 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_18,
		ROUND(AVG(CASE WHEN rdg_year = 2019 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_19,
		ROUND(AVG(CASE WHEN rdg_year = 2020 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_20,
		ROUND(AVG(CASE WHEN rdg_year = 2021 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_21,
		ROUND(AVG(CASE WHEN rdg_year = 2022 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_22,
		ROUND(AVG(CASE WHEN rdg_year = 2023 THEN tmp_max::numeric ELSE Null END), 2) AS hgh_tmp_23	
	FROM data_transform_prelim_2
	GROUP BY rdg_month, station_name, state
)

SELECT *
FROM data_transform_final
ORDER BY station_name, rdg_month;
