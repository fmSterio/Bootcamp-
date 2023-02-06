import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests
from prefect_gcp import GcpCredentials



@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """ Download trip data from GCS """
    gcs_path = f"data/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("bootcamp-w2")
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    
    return Path(gcs_path)

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df.passenger_count.isna().sum()}")
    #df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df.passenger_count.isna().sum()}")
    return df

@task
def write_bq(df: pd.DataFrame) -> None:
    """Write the data to BigQuery"""
    
    gcp_credentials_block = GcpCredentials.load("bootcamp-w2-creds")    
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="dtc-de-375317",
        if_exists="append",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=50000
    )
    


@flow
def etl_gcp_to_bg(color:str, year:int, month:int):

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


@flow(log_prints=True)
def etl_parent_flow(color="yellow", year=2021, month=[2,3]):
    """Parent flow to run the ETL"""
    for m in month:
        etl_gcp_to_bg(color=color, year=year,month=m)


if __name__ == '__main__':
    etl_gcp_to_bg()