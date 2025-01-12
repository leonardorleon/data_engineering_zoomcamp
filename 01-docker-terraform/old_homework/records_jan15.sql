-- How many records in January 15
SELECT COUNT(*)
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime::date=date('2021-01-15');