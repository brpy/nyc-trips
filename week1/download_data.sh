mkdir -p data
cd data
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
gzip -d green_tripdata_2019-01.csv.gz
