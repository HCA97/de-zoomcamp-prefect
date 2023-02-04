import tempfile as tmp
from datetime import timedelta

import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from prefect import task, flow
from prefect.tasks import task_input_hash



@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)
    

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    except:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(f"Total Number of Rows: {len(df)}")
    return df

@task(retries=3)
def upload_data(df: pd.DataFrame, dataset_name: str) -> str:
    gcp_cloud_storage_bucket_block = GcsBucket.load("de-zoomcamp-bucket")
    with tmp.NamedTemporaryFile(suffix=".parquet") as f:
        df.to_parquet(f.name, compression="gzip")
        print(f'Succesfully save the file to {f.name}')
        gcp_cloud_storage_bucket_block.upload_from_path(from_path=f.name, to_path=dataset_name)

    return dataset_name
    

@flow(name='GitHub-to-GCS', log_prints=True)
def web_to_gcs(month: int, year: int, color: str) -> str:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    
    df = download_data(dataset_url)
    df = clean_data(df)
    return upload_data(df, f'{dataset_file}.parquet')

@flow(name='Prefect-ETL', log_prints=True)
def main_flow(months: list, year: int, color: str) -> None:
    dataset_names = [
         web_to_gcs(month, year, color) for month in months
    ]
    print(dataset_names)

if __name__ == "__main__":
    # question 3
    months = [2, 3]
    color = 'yellow'
    year = 2019
    main_flow(months, year, color)

