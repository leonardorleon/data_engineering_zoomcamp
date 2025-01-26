## Module 2 Homework

ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format, please include these directly in the README file of your repository.

> In case you don't get one option exactly, select the closest one 

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

To get a `wget`-able link, use this prefix (note that the link itself gives 404):

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.


As a hint, Kestra makes that process really easy:
1. You can leverage the backfill functionality in the to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from `2021-01-01` to `2021-07-31`. Also, make sure to do the same for both `yellow` and `green` taxi data (select the right service in the `taxi` input).
2. Alternatively, run the flow manually for each of the seven months of 2021 for both `yellow` and `green` taxi data. Challenge for you: find out how to loop over the combination of Year-Month and `taxi`-type using `ForEach` task which triggers the flow for each combination using a `Subflow` task.


---------------------------------
## Note
I used the backfilling option in kestra, though I played around with the for each task, and I figured something like this could be used:

```
id: test_looping
namespace: de_zoomcamp

variables:
  years: ['2019','2020','2021']
  months: ['01','02','03']


tasks:
  - id: loop_through_years
    type: "io.kestra.plugin.core.flow.ForEach"
    values: "{{vars.years}}"
    tasks: 
      - id: loop_through_months
        type: "io.kestra.plugin.core.flow.ForEach"
        values: "{{vars.months}}"
        tasks:
          - id: execute_year_month_combo
            type: io.kestra.plugin.core.log.Log
            message: "{{parent.taskrun.value}} ~ {{taskrun.value}}"
```
---------------------------------

### Quiz Questions

Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MB  <----------- The file in GCS has this size
- 134.5 MB
- 364.7 MB
- 692.6 MB

2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- `green_tripdata_2020-04.csv`  <--------------- It would look like this
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
- 13,537.299
- 24,648,499    <------------- 24.648.656
- 18,324,219
- 29,430,127

```
SELECT 
  EXTRACT(ISOYEAR FROM tpep_pickup_datetime) as yr
  ,COUNT(*) 
FROM `sunlit-amulet-341719.zoomcamp.yellow_tripdata`
where tpep_pickup_datetime > '2020-01-01' 
      AND tpep_pickup_datetime < '2021-01-01'
GROUP BY 1
;
```


4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- 1,734,051 <---------- 1.734.039
- 1,342,034


```
SELECT 
  EXTRACT(ISOYEAR FROM lpep_pickup_datetime) as yr
  ,COUNT(*) 
FROM `sunlit-amulet-341719.zoomcamp.green_tripdata`
where lpep_pickup_datetime > '2020-01-01' 
      AND lpep_pickup_datetime < '2021-01-01'
GROUP BY 1
;
```

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- 1,925,152     <--------- 1.925.130
- 2,561,031

```
SELECT 
  EXTRACT(ISOYEAR FROM tpep_pickup_datetime) as yr
  ,EXTRACT(MONTH FROM tpep_pickup_datetime) as mo
  ,COUNT(*) 
FROM `sunlit-amulet-341719.zoomcamp.yellow_tripdata`
where EXTRACT(ISOYEAR FROM tpep_pickup_datetime) = 2021
  and EXTRACT(MONTH FROM tpep_pickup_datetime) = 3
GROUP BY 1,2
;
```

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration       <------- I'd add a timezone property as shown in the docs: https://kestra.io/docs/workflow-components/triggers/schedule-trigger
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw2
* Check the link above to see the due date

## Solution

Will be added after the due date
