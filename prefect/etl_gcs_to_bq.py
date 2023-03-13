from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_data_gcs(color: str, year: int, month: int) -> Path:
	"""Downloads NY taxi data from GCS"""

	gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
	gcs_block = GcsBucket.load("gcs-bucket")
	gcs_block.get_directory(from_path=gcs_path, local_path=f'data/from_gcs')

	return Path('data/from_gcs')

@task()
def transform_data(path: Path) -> pd.DataFrame:
	"""Cleans data"""

	df = pd.read_parquet(path)
	print(f'Pre: missing passenger count: {df["passenger_count"].isna().sum()}')
	df['passenger_count'].fillna(0, inplace=True)
	print(f'Post: missing passenger count: {df["passenger_count"].isna().sum()}')

	return df

@task()
def write_data_bq(df: pd.DataFrame) -> None:
	"""Writes DataFrame to BigQuery"""

	credentials_block = GcpCredentials.load('gcp-service-account-credentials')
	df.to_gbq(destination_table='ny_taxi_data.rides', project_id='cobalt-duality-378103', credentials=credentials_block.get_credentials_from_service_account(), chunksize=500000, if_exists='append')

	return

@flow()
def etl_gcs_to_bq():
	"""The main ETL function to load data into BigQuery"""
	color = 'yellow'
	year = 2021
	month = 1

	path = extract_data_gcs(color, year, month)
	df = transform_data(path)
	write_data_bq(df)

	return

if __name__ == '__main__':
	etl_gcs_to_bq()