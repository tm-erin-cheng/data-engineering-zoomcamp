from pathlib import Path
import pandas as pd
from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_data(url: str) -> pd.DataFrame:
	"""Reads taxi data from the web into a pandas DataFrame"""

	df = pd.read_csv(url)
	return df
    
@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
	"""Fixes dtype issues"""
	df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
	df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

	print(df.head(2))
	print(f'columns: {df.dtypes}')
	print(f'rows: {len(df)}')

	return df

@task()
def write_data_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
	"""Writes DataFrame out locally as a parquet file"""
	path = Path(f'data/{color}/{dataset_file}.parquet')
	df.to_parquet(path, compression='gzip')

	return path

@task(retries=3)
def write_data_gcs(path: Path) -> None:
	"""Uploads the local parquet file to GCS"""

	gcs_block = GcsBucket.load("gcs-bucket")
	gcs_block.upload_from_path(from_path=path, to_path=path)

	return

@flow()
def etl_web_to_gcs() -> None:
	"""The main ETL flow to get NY taxi data and load it to a GCS bucket"""
	color = 'yellow'
	year = 2021
	month = 1
	dataset_file = f'{color}_tripdata_{year}-{month:02}'
	dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

	df = fetch_data(dataset_url)
	df_clean = clean_data(df)
	path = write_data_local(df_clean, color, dataset_file)
	write_data_gcs(path)

if __name__ == '__main__':
	etl_web_to_gcs()