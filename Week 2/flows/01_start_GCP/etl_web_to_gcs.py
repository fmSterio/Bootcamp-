import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=20))
def fetch(dataset_url: str) -> Path():
    """Read the data from web into pandas DataFrame."""

    ## pd.read_csv is raising an error SSL: CERTIFICATE_VERIFY_FAILED error
    #df = pd.read_csv(dataset_url
    file_name = Path(f"data/{dataset_url.split('/')[-1]}")

    if not file_name.exists(): 
        try:
            response = requests.get(dataset_url)
            with open(file_name, 'wb') as f:
                f.write(response.content)
                return file_name
        except Exception as e:
            print(e)
    else:
        print("File already exists")
        return file_name

@task
def clean(local_url: str) -> pd.DataFrame:
    """Clean the data, fix dtypes issue"""
    df = pd.read_csv(local_url)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.store_and_fwd_flag = df.store_and_fwd_flag.astype('bool')
    return df

@task
def write_local(df: pd.DataFrame,dataset_file:str) -> Path:
    """Write the data to local"""
    filename_name = Path(f"data/{dataset_file}.parquet")
    if not filename_name.exists():
        try:
            df.to_parquet(filename_name,compression='gzip')
            return filename_name
        except Exception as e:
            print(e, "Error writing to local")
    else:
        print("File already exists")
        return filename_name

@task
def write_gcs(path: Path) -> None:
    """Write the data to GCS"""
    gcp_block = GcsBucket.load("bootcamp-w2") 
    gcp_block.upload_from_path(
        from_path=path,
        to_path=path)
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str ) -> None:
    """The main flow for the ETL process. """
    
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    local_url = fetch(dataset_url)
    df = clean(local_url) # clean the data
    path=write_local(df,dataset_file) # write the data to local
    write_gcs(path) # write the data to GCS

@flow()
def etl_parent_flow(months: list[int]=[1,2], year: int = 2021, color: str = "yellow" ):
    
    for month in months:
        etl_web_to_gcs(year, month, color)
        


if __name__ == '__main__':
    year = 2021
    month = [1,2,3]
    color = "yellow"
    etl_parent_flow(months=month, year=year, color=color)
    
    