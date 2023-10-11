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
),

data_transform_final AS (
	SELECT
		station,
		rdg_year,
		rdg_month,
		slp_3hr_diff,
		COUNT(date) AS month_count
	FROM data_transform_prelim
	GROUP BY station, rdg_year, rdg_month, slp_3hr_diff
)

SELECT
	ls.year,
	ls.region,
	ls.sub_region,
	ls.state,
	ls.station_name,
	da.slp_3hr_diff,
	SUM(CASE WHEN da.rdg_month BETWEEN 1 AND 12 THEN da.month_count ELSE 0 END) AS ttl_year,
	SUM(CASE WHEN da.rdg_month = 1 THEN da.month_count ELSE 0 END) AS jan_cnt,
	SUM(CASE WHEN da.rdg_month = 2 THEN da.month_count ELSE 0 END) AS feb_cnt,
	SUM(CASE WHEN da.rdg_month = 3 THEN da.month_count ELSE 0 END) AS mar_cnt,
	SUM(CASE WHEN da.rdg_month = 4 THEN da.month_count ELSE 0 END) AS apr_cnt,
	SUM(CASE WHEN da.rdg_month = 5 THEN da.month_count ELSE 0 END) AS may_cnt,
	SUM(CASE WHEN da.rdg_month = 6 THEN da.month_count ELSE 0 END) AS jun_cnt,
	SUM(CASE WHEN da.rdg_month = 7 THEN da.month_count ELSE 0 END) AS jul_cnt,
	SUM(CASE WHEN da.rdg_month = 8 THEN da.month_count ELSE 0 END) AS aug_cnt,
	SUM(CASE WHEN da.rdg_month = 9 THEN da.month_count ELSE 0 END) AS sep_cnt,
	SUM(CASE WHEN da.rdg_month = 10 THEN da.month_count ELSE 0 END) AS oct_cnt,
	SUM(CASE WHEN da.rdg_month = 11 THEN da.month_count ELSE 0 END) AS nov_cnt,
	SUM(CASE WHEN da.rdg_month = 12 THEN da.month_count ELSE 0 END) AS dec_cnt
FROM loc_subset ls
LEFT OUTER JOIN data_transform_final da
    ON ls.station = da.station AND ls.year = da.rdg_year
WHERE ls.year BETWEEN 2022 AND 2022
    AND ls.region = 'MIDWEST'
GROUP BY ls.year, ls.region, ls.sub_region, ls.state, ls.station_name, da.slp_3hr_diff
ORDER BY ls.station_name, da.slp_3hr_diff;
