import logging
import os
from io import BytesIO
from zipfile import ZipFile

import polars as pl
import requests
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv('../.env')
# Set the logging level for all azure-* libraries
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('azure')
logger.setLevel(logging.ERROR)
logger_normal = logging.getLogger(__name__)

DOWNLOAD_URIS = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
]

LANDING_ZONE_PATH = os.getenv('LANDING_ZONE_PATH')
ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')
BRONZE_CONTAINER = os.getenv('STAGING_PATH')
SILVER_CONTAINER = os.getenv('HISTORICAL_PATH')
GOLD_CONTAINER = os.getenv('DW_PATH')


class ETLPipeline:
    """
    Mock class that simulates a modulith to process from landing to bronze, silver and gold.
    It takes the assumption that we process one file at a time for simplification (mostly because
    I did not want to write asyncio calls and deal with the connections in the ADLSGenClient)
    """

    def __init__(self) -> None:
        self.adlsgen2_client = DataLakeServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net/", credential=AzureCliCredential()
        )
        self.bronze_client = self.adlsgen2_client.get_directory_client(file_system=LANDING_ZONE_PATH, directory='/')

    def upload_to_landing(self, uri: str):
        """_summary_

        Args:
            uri (str): _description_
        """

        try:
            response = requests.get(url=uri)
            response.raise_for_status()
            with ZipFile(BytesIO(response.content), 'r') as zip_ref:
                # Assuming there is only one file in the zip archive
                file_name = zip_ref.namelist()[0]
                # Read the CSV file into a polars dataframe
                df = pl.read_csv(zip_ref.open(file_name).read())

                # Save the DataFrame as a parquet file in blob
                file_name = f"{file_name.lower().split('.csv')[0]}.parquet"
                path_name = f"../data/input/{file_name}"
                df.write_parquet(path_name, use_pyarrow=True, compression='lz4')

                file_client = self.bronze_client.get_file_client(file_name)

                with open(file=path_name, mode="rb") as data:
                    file_client.upload_data(data, overwrite=True)
                logger_normal.info(f"{file_name} uploaded to adlsgen2")
                self.file_name = file_name
        except requests.exceptions.HTTPError as e:
            logger_normal.error(f"Failed to download {uri}. HTTP error occurred: {e}")
        except Exception as e:
            logger_normal.error(f"An error occurred processing {uri}: {e}")

    def raw_to_bronze(self):
        try:
            storage_options_raw = {"account_name": ACCOUNT_NAME, "anon": False}
            storage_options_raw_delta = {"account_name": ACCOUNT_NAME, "use_azure_cli": "True"}
            logger_normal.info(f"Reading {self.file_name}")
            df = pl.read_parquet(
                source=f'abfs://{LANDING_ZONE_PATH}/{self.file_name}', storage_options=storage_options_raw
            )
            logger_normal.info(f"Converting {self.file_name} to delta")
            df.write_delta(
                target=f'abfs://{BRONZE_CONTAINER}/', mode='append', storage_options=storage_options_raw_delta
            )
        except Exception:
            logger_normal.error(f"Failed to conver to delta {self.file_name}")

    def bronze_to_silver(self):
        return True

    def silver_to_gold(self):
        return True


if __name__ == "__main__":
    etl_workflow = ETLPipeline()
    for uri in DOWNLOAD_URIS:
        etl_workflow.upload_to_landing(uri=uri)
        etl_workflow.raw_to_bronze()
