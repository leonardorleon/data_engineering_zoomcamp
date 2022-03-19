-- most expensive pair of locations (pick up / drop off)
SELECT
	CONCAT(coalesce(pu_zones."Zone",'Unknown'), ' / ', coalesce(do_zones."Zone", 'Unknown')) as zone_pairs,
	AVG(taxi."total_amount") as avg_cost
FROM yellow_taxi_trips as taxi

LEFT JOIN ny_taxi_zones as pu_zones
ON taxi."PULocationID"=pu_zones."LocationID"

LEFT JOIN ny_taxi_zones as do_zones
ON taxi."DOLocationID"=do_zones."LocationID"

GROUP BY zone_pairs
ORDER BY avg_cost DESC
LIMIT 1;