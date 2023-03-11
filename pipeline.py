pgcli -h localhost -p 5432 -u root -d ny_taxi

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13

docker network create pg-network

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4

python3 ingest_data.py \
  --username=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --database=ny_taxi \
  --table_name=yellow_taxi_data \
  --url=${URL}

docker build -t ingest_taxi_data:v001 .

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
URL2="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
docker run -it \
  --network=pg-network \
ingest_taxi_data:v001 \
    --username=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --database=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}