# Homework Module 1 

## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1 <--
- 24.2.1
- 23.3.1
- 23.2.1

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433 
- postgres:5432
- db:5432 <-- pgadmin is in the same network as postgres, so it should still use 5432, even though the host machine connected 5433 to 5432 in the container.

##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.


### To ingest green taxi data I use an ingestion script

Download this data and put it into Postgres using the modified script.

```bash
docker build -t taxi_ingest:v002 .
```

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

docker run -it \
  --network=dockerfiles_default \
  taxi_ingest:v002 \
    --user=root \
    --password=root \
    --host=dockerfiles-pgdatabase-1 \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips_2019 \
    --url=${URL}
```

### To ingest green zone data I use jupyter notebook [Green data notebook](green_data.ipynb)


## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189 <--
- 104,793;  201,407;  110,612;  27,831;  35,281
- 104,793;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202

```sql
with trip_cat as (
	select
		CASE 
			WHEN gt.trip_distance <= 1 THEN 'Up to 1 mile'
			WHEN gt.trip_distance > 1 and gt.trip_distance <= 3 THEN 'between 1 and 3 miles'
			WHEN gt.trip_distance > 3 and gt.trip_distance <= 7 THEN 'between 3 and 7 miles'
			WHEN gt.trip_distance > 7 and gt.trip_distance <= 10 THEN 'between 7 and 10 miles'
			WHEN gt.trip_distance > 10 THEN 'Over 10 miles'
			ELSE NULL
		END AS trip_category
	from green_taxi_trips_2019 as gt
	where gt.lpep_pickup_datetime >= '2019-10-01' 
		and gt.lpep_dropoff_datetime < '2019-11-01'
)
select 
	trip_category, 
	count(*)
from trip_cat
group by trip_category
order by trip_category asc
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31 <--

```sql
select
	date(lpep_pickup_datetime), trip_distance
from green_taxi_trips_2019
order by trip_distance desc
limit 1
```


## Question 5. Three biggest pickup zones

Which where the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights <--
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park


```sql
select 
	date(gt.lpep_pickup_datetime) 				as dt,
	zn."Borough" 								as borough, 
	zn."Zone"									as zon,
	cast(sum(gt.total_amount) as int)			as sum_total
from green_taxi_trips_2019 as gt
left join green_ny_taxi_zones as zn
	on gt."PULocationID" = zn."LocationID"
where date(gt.lpep_pickup_datetime) = '2019-10-18'
group by 
	dt, 
	borough, 
	zon
order by sum(gt.total_amount) desc
limit 3
```

## Question 6. Largest tip

For the passengers picked up in Ocrober 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport <--
- East Harlem North
- East Harlem South

```sql
select
	date_part('year',gt.lpep_pickup_datetime)	as pu_year,
	date_part('month',gt.lpep_pickup_datetime)	as pu_month,
	pu_zn."Zone" 	as pu_zone,
	do_zn."Zone"	as do_zone,
	gt.tip_amount
from green_taxi_trips_2019 as gt
left join green_ny_taxi_zones as pu_zn
	on gt."PULocationID" = pu_zn."LocationID"
left join green_ny_taxi_zones as do_zn
	on gt."DOLocationID" = do_zn."LocationID"
where 
	date_part('year',gt.lpep_pickup_datetime) = '2019' 
	and date_part('month',gt.lpep_pickup_datetime) = '10'
	and pu_zn."Zone" = 'East Harlem North'
order by gt.tip_amount desc
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy <--
- terraform import, terraform apply -y, terraform rm


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1