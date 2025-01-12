# Count records

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

**Solution**

```sql
-- How many records in January 15
SELECT COUNT(*)
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime::date=date('2021-01-15');
```

answer: 53024

# Largest tip for each day

Find the largest tip for each day. On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

**Solution**

```sql
-- Largest tip per day
SELECT tpep_pickup_datetime as pickup_day, max(tip_amount) as max_tip
FROM yellow_taxi_trips
GROUP BY pickup_day
ORDER BY max_tip DESC
LIMIt 1;
```

answer: "2021-01-20 11:22:05" and max tip 1140.44

# Most popular destination

What was the most popular destination for passengers picked up in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

**Solution**

```sql
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
```

Answer: "Upper East Side South"	97

# Most expensive locations

What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

**Solution**

```sql
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
``` 

Answer: "Alphabet City / Unknown"	2292.4