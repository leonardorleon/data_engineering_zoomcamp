CREATE OR REPLACE EXTERNAL TABLE `sunlit-amulet-341719.zoomcamp.external_yellow_tripdata` 
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-de-zoomcamp-bucket-sunlit-amulet-341719/yellow_tripdata_2019-*.csv','gs://kestra-de-zoomcamp-bucket-sunlit-amulet-341719/yellow_tripdata_2020-*.csv']
);

CREATE OR REPLACE TABLE `sunlit-amulet-341719.zoomcamp.yellow_tripdata_partitoned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `sunlit-amulet-341719.zoomcamp.external_yellow_tripdata` ;

--NOTE:  Let's work on ML parts. This time we would like to predict the tip amount. 

-- SELECT THE COLUMNS INTERESTED FOR YOU
SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
FROM `sunlit-amulet-341719.zoomcamp.yellow_tripdata_partitoned` WHERE fare_amount != 0;

/*
NOTE: Let's do some feature preprocessing. There are two types, automatic or manual. 

automatic: here there are some automatic transformations such as standatization of numeric fields, one-hot encoding of categoric fields and multi-hot encoding of arrays. There are more as well.

manual: for manual preprocessing there are multiple options such as bucketization, polynomial expand and many other options which can be looked into. 

*/

-- to facilitate the automatic preprocessing, let's cast the various columns to appropriate variable types. For example, the pick up and drop off locations are originally integers, but really they should be a categoric variable, so a string will fit better. 

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `sunlit-amulet-341719.zoomcamp.yellow_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
  SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
  CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
  FROM `sunlit-amulet-341719.zoomcamp.yellow_tripdata_partitoned` WHERE fare_amount != 0
);

/*
Now we create a "tip model" since that's what we're trying to predict. 

The model type will be a linear regression, the input label is chosen as the tip amount and we're letting it do the training and test split automatically. 
*/

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `sunlit-amulet-341719.zoomcamp.tip_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
  *
FROM
  `sunlit-amulet-341719.zoomcamp.yellow_tripdata_ml`
WHERE
  tip_amount IS NOT NULL;

/*
Now we can check on the feature information. 
*/

-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `sunlit-amulet-341719.zoomcamp.tip_model`);

/*
We can see how the model does against training data
*/

-- EVALUATE THE MODEL
SELECT
  *
FROM
  ML.EVALUATE(MODEL `sunlit-amulet-341719.zoomcamp.tip_model`,
  (
  SELECT
  *
  FROM
  `sunlit-amulet-341719.zoomcamp.yellow_tripdata_ml`
  WHERE
  tip_amount IS NOT NULL
  ));

/*
now let's predict the tip amount using the dataset. it creates an extra column which is the tip amount. 
*/

-- PREDICT THE MODEL
SELECT
  *
FROM
  ML.PREDICT(MODEL `sunlit-amulet-341719.zoomcamp.tip_model`,
(
  SELECT
  *
  FROM
  `sunlit-amulet-341719.zoomcamp.yellow_tripdata_ml`
  WHERE
  tip_amount IS NOT NULL
));

/*
There is also an explain and predict option which will give extra info about the most important category features. 
*/

-- PREDICT AND EXPLAIN
SELECT
  *
FROM
  ML.EXPLAIN_PREDICT(MODEL `sunlit-amulet-341719.zoomcamp.tip_model`,
(
  SELECT
  *
  FROM
  `sunlit-amulet-341719.zoomcamp.yellow_tripdata_ml`
  WHERE
  tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));

/*
Since the model is not optimal, let's do some hyper parameter tuning. 

This is only a few of them, there are many more options in the documentation for linear regression in BQ.
*/

-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `sunlit-amulet-341719.zoomcamp.tip_hyperparam_model`
OPTIONS
  (model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT',
  num_trials=5,
  max_parallel_trials=2,
  l1_reg=hparam_range(0, 20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT
  *
FROM
  `sunlit-amulet-341719.zoomcamp.yellow_tripdata_ml`
WHERE
  tip_amount IS NOT NULL;


/*
Let's extract the model. 


    gcloud auth login
    
    bq --project_id sunlit-amulet-341719 extract -m zoomcamp.tip_model gs://sunlit_ml_model/tip_model     
    
    mkdir /tmp/model
    
    gsutil cp -r gs://sunlit_ml_model/tip_model /tmp/model
    
    mkdir -p serving_dir/tip_model/1
    
    cp -r /tmp/model/tip_model/* serving_dir/tip_model/1
    
    docker pull tensorflow/serving
    
    docker run -p 8501:8501 --mount type=bind,source=`pwd`/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &
    
    curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}'   -X POST http://localhost:8501/v1/models/tip_model:predict
    
    http://localhost:8501/v1/models/tip_model


*/