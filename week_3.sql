CREATE OR REPLACE EXTERNAL TABLE deft-citizen-374601.Week_3_homework.week_3_external
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-aminu/fhv_/fhv_tripdata_2019-*.csv.gz']
);

SELECT * FROM deft-citizen-374601.Week_3_homework.week_3
LIMIT 100;


--Q1 What is the count for fhv vehicle records for year 2019?
SELECT COUNT(*) FROM deft-citizen-374601.Week_3_homework.week_3_external;

--Q2 Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT( DISTINCT affiliated_base_number) FROM deft-citizen-374601.Week_3_homework.week_3_external;
SELECT COUNT( DISTINCT affiliated_base_number) FROM deft-citizen-374601.Week_3_homework.week_3;

--Q3 How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(1) FROM deft-citizen-374601.Week_3_homework.week_3_external
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

--Q4 What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
CREATE OR REPLACE TABLE deft-citizen-374601.Week_3_homework.week_3_external_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM deft-citizen-374601.Week_3_homework.week_3_external;

CREATE OR REPLACE TABLE deft-citizen-374601.Week_3_homework.week_3_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM deft-citizen-374601.Week_3_homework.week_3;

--Q5 Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
SELECT DISTINCT affiliated_base_number FROM deft-citizen-374601.Week_3_homework.week_3
WHERE pickup_datetime BETWEEN CAST('2019-03-01' AS TIMESTAMP) AND CAST('2019-04-01' AS TIMESTAMP);

SELECT  DISTINCT affiliated_base_number FROM deft-citizen-374601.Week_3_homework.week_3_partitioned_clustered
WHERE pickup_datetime BETWEEN CAST('2019-03-01' AS TIMESTAMP) AND CAST('2019-04-01' AS TIMESTAMP);






