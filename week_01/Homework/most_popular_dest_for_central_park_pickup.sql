SELECT
	COALESCE(do_zones."Zone",'Unknown')as do_zone,
	count(*) as do_count
FROM yellow_taxi_trips as taxi

INNER JOIN ny_taxi_zones as pu_zones
ON taxi."PULocationID"=pu_zones."LocationID"

LEFT JOIN ny_taxi_zones as do_zones
ON taxi."DOLocationID"=do_zones."LocationID"

WHERE pu_zones."Zone" ilike '%central park%'
AND taxi.tpep_pickup_datetime::date=date('2021-01-14')

GROUP BY do_zone
ORDER BY do_count DESC
limit 1;