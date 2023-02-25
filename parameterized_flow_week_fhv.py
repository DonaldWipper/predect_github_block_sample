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
    df = pd.read_csv(dataset_url)
    return df


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
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""

    Path("data/fhv").mkdir(parents=True, exist_ok=True)

    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


##########

@task(retries=3)
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    df.to_gbq(
        destination_table=f"dbt.fhv",
        project_id="zoomcamp2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    print(gcs_path)
    gcs_block = GcsBucket.load("zoom-gcs-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return pd.read_parquet(Path(f"../data/{gcs_path}"))



@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    print(dataset_url)
    df = fetch(dataset_url)

    # df_clean = clean(df)
    path = write_local(df, dataset_file)
    del df
    write_gcs(path)


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    df = extract_from_gcs(color, year, month)
    write_bq(df, color)


@flow(log_prints=True)
def etl_parent_flow(
        months: list[int] = [1, 2, 3], year: int = 2019
):
    for month in months:
        print(month)
        etl_web_to_gcs(year, month)
        etl_gcs_to_bq(year, month)


if __name__ == "__main__":
    etl_parent_flow(list(range(1, 13)))
