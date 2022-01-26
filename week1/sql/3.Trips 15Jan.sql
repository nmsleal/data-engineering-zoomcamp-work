select count(*)
from yellow_taxi_trips
where date(tpep_pickup_datetime) = '20210115'