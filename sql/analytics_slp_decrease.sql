WITH data_transform_prelim AS (
	SELECT
		obs.station,
		obs.date,
		EXTRACT(YEAR from obs.date) AS rdg_year,
		EXTRACT(MONTH from obs.date) AS rdg_month,
		EXTRACT(DAY from obs.date) AS rdg_day,
		ROUND(obs.slp::numeric - LAG(obs.slp, 3) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_3hr_diff,
		ROUND(obs.slp::numeric - LAG(obs.slp, 6) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_6hr_diff,
		ROUND(obs.slp::numeric - LAG(obs.slp, 24) OVER(PARTITION BY obs.station ORDER BY obs.date)::numeric, 2) AS slp_24hr_diff
	FROM observations obs
	WHERE EXTRACT(YEAR from obs.date) BETWEEN 2013 AND 2022
		AND obs.source IN ('6', '7')
		AND obs.report_type IN ('FM-15')
		AND obs.slp BETWEEN 20.00 AND 35.00
		AND obs.prp <= 10.00
),

data_transform_final AS (
	SELECT
		station,
		rdg_year,
		rdg_month,
		rdg_day,
		MIN(slp_3hr_diff) AS slp_3hr_min,
		MIN(slp_6hr_diff) AS slp_6hr_min,
		MIN(slp_24hr_diff) AS slp_24hr_min
	FROM data_transform_prelim
	GROUP BY station, rdg_year, rdg_month, rdg_day
),

data_aggregate_1 AS (
	SELECT
		station,
		rdg_year,
		rdg_month,
		COUNT(DISTINCT rdg_day) AS day_count
	FROM data_transform_final
	WHERE slp_3hr_min BETWEEN -5.0 AND -0.15
		OR slp_6hr_min BETWEEN -5.0 AND -0.25
	    OR slp_24hr_min BETWEEN -5.0 AND -0.50
	GROUP BY station, rdg_year, rdg_month
),

data_aggregate_2 AS (
    SELECT
        ls.year,
        ls.region,
        ls.sub_region,
        ls.state,
        ls.station_name,
        ls.lat,
        ls.lon,
        SUM(CASE WHEN rdg_month BETWEEN 1 AND 12 THEN day_count ELSE 0 END) AS ttl_year
    FROM loc_subset ls
    LEFT OUTER JOIN data_aggregate_1 da1
        ON ls.station = da1.station AND ls.year = da1.rdg_year
    WHERE ls.year BETWEEN 2013 AND 2022
    GROUP BY ls.year, ls.region, ls.sub_region, ls.state, ls.station_name, ls.lat, ls.lon
)

SELECT
    region,
    sub_region,
    state,
    station_name,
    lat,
    lon,
    ROUND(AVG(ttl_year), 0) AS ttl_year_avg
FROM data_aggregate_2
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4;