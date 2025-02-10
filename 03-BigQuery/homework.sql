CREATE OR REPLACE EXTERNAL TABLE `playground-385016.ny_taxi_zoomcamp.yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-zoomcamo/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `playground-385016.ny_taxi_zoomcamp.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `playground-385016.ny_taxi_zoomcamp.yellow_tripdata`;

--# Question 1
select count(1) from `playground-385016.ny_taxi_zoomcamp.fhv_nonpartitioned_tripdata`;
--20332093

--# Question 2
SELECT count(distinct PULocationID) FROM `playground-385016.ny_taxi_zoomcamp.yellow_tripdata`;

SELECT count(distinct PULocationID) FROM `playground-385016.ny_taxi_zoomcamp.fhv_nonpartitioned_tripdata`;

--0 MB for the External Table and 155.12 MB for the Materialized Table

SELECT PULocationID, DOLocationID FROM `playground-385016.ny_taxi_zoomcamp.fhv_nonpartitioned_tripdata`;

--BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.


--Question 4
SELECT count(1)  FROM `playground-385016.ny_taxi_zoomcamp.fhv_nonpartitioned_tripdata`
where fare_amount = 0
;
--8333

--Question 5

CREATE OR REPLACE TABLE `playground-385016.ny_taxi_zoomcamp.fhv_partitioned_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `playground-385016.ny_taxi_zoomcamp.yellow_tripdata`
);


--Question 6

SELECT distinct VendorID FROM `playground-385016.ny_taxi_zoomcamp.fhv_nonpartitioned_tripdata`
where tpep_dropoff_datetime>='2024-03-01' and tpep_dropoff_datetime<='2024-03-15'
;


SELECT distinct VendorID FROM `playground-385016.ny_taxi_zoomcamp.fhv_partitioned_tripdata`
where tpep_dropoff_datetime>='2024-03-01' and tpep_dropoff_datetime<='2024-03-15'
;

--310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

