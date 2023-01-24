




 

# docker network
docker network create pg-network




## postgres 
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v "$(pwd)"/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-admin \
  postgres:13

  ## pgadmin

docker run -it \
 -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
 -e PGADMIN_DEFAULT_PASSWORD="root" \
 -p 8080:80 \
 --network=pg-network \
 --name pgadmin \
 dpage/pgadmin4
 

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_data \
 --url=${URL}

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



docker-coompose up -d  (-d for detached mode, it runs the containers in the background and give us back terminal prompt)
docker-compose down  (shoutdown the containers)


docker run -it --entrypoint bash python:3.9

--entrypoint bash