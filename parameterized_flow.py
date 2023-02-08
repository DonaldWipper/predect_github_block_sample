from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    print(dataset_url)
    df = pd.read_csv(dataset_url)
    print('finish fetch')
    return df


@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.yellow_rides",
        project_id="zoomcamp2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return pd.read_parquet(Path(f"../data/{gcs_path}"))


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    # df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs-block")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=300)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    df = extract_from_gcs(color, year, month)
    write_bq(df)


@flow(log_prints=True)
def etl_parent_flow(
        months: list[int] = [1, 2, 3], year: int = 2020, color: str = "green"
):
    for month in months:
        print(month)
        etl_web_to_gcs(year, month, color)
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)
