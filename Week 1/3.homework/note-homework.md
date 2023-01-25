
docker network create pg-network


 docker build -t taxi_ingest:v001 .

 docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pg-admin \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_data \
        --url=${URL}



URL1="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
URL2="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

python3 ingest_hw_data.py \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name1=green_trip_data \
    --table_name2=taxi_zone_lookup \
    --url1=${URL1} \
    --url2=${URL2}


docker build -t taxi_ingest_data_hw:v001 .
docker run -it \
    --network=pg-network_hw \
    taxi_ingest_data_hw:v001 \
        --user=root \
        --password=root \
        --host=localhost \
        --port=5432 \
        --db=ny_taxi \
        --table_name1=green_trip_data \
        --table_name2=taxi_zone_lookup \
        --url1=${URL1} \
        --url2=${URL2} 




 

------------------------------------------------------------------------------------------------------------------------------------

SQL


SELECT count(1) as nr_of_trips
FROM green_trip_data
where date(lpep_pickup_datetime) = '2019-01-15' and date(lpep_dropoff_datetime) = '2019-01-15';

SELECT date(lpep_pickup_datetime) as day_, max(trip_distance) as distance
FROM green_trip_data
group by day_
order by distance DESC;

SELECT count(1), passenger_count
from green_trip_data
where date(lpep_pickup_datetime) = '2019-01-01' and passenger_count in (2,3) 
group by passenger_count;

select doz."Zone" as dropoff_zone, max(gtd.tip_amount) as tips_per_zone
from green_trip_data gtd 
inner join taxi_zone_lookup puz 
on gtd."PULocationID"= puz."LocationID" 
inner join taxi_zone_lookup doz  
on gtd."DOLocationID"= doz."LocationID"
where puz."Zone"='Astoria'
group by doz."Zone" 
order by tips_per_zone DESC;
