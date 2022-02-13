-- Question 1: What is count for fhv vehicles data for year 2019
SELECT sum(total_rows)
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'fhv_tripdata_partitioned'
-- 42084899

-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019
SELECT count(distinct dispatching_base_num)
FROM `dtc-de-339219.trips_data_all.fhv_tripdata_partitioned`
WHERE extract(year FROM pickup_datetime) = 2019
-- 792

-- Question 4: What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
SELECT count(*)
FROM `dtc-de-339219.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE DATE(pickup_datetime) between "2019-01-01" and "2019-03-31"
    and dispatching_base_num in ("B00987","B02060","B02279")
-- 26647 trips, 400MB estimated, 142MB processed