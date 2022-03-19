SELECT tpep_pickup_datetime as pickup_day, max(tip_amount) as max_tip
FROM yellow_taxi_trips
GROUP BY pickup_day
ORDER BY max_tip DESC
LIMIt 1;