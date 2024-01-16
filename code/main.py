import logging
import os
import time
from io import BytesIO
from zipfile import ZipFile

import polars as pl
import requests
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable
from dotenv import load_dotenv

load_dotenv('../.env')
# Set the logging level for all azure-* libraries
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
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
BRONZE_CONTAINER = os.getenv('APPEND_LAYER')
SILVER_CONTAINER = os.getenv('HISTORICAL_PATH')
GOLD_CONTAINER = os.getenv('DW_PATH')


class ETLPipeline:
    """Mock class that simulates a modulith to process from landing to bronze, silver and gold.
    
    It takes the assumption that we process one file at a time for simplification (mostly because
    I did not want to write asyncio calls and deal with the connections in the ADLSGenClient)
    """

    def __init__(self) -> None:
        self.adlsgen2_client = DataLakeServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net/", credential=AzureCliCredential()
        )
        self.landing_client = self.adlsgen2_client.get_directory_client(file_system=LANDING_ZONE_PATH, directory='/')

    def upload_to_landing(self, uri: str):
        """Uploads the data to the landing zone.

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
                raw_df = pl.read_csv(zip_ref.open(file_name).read())

                # Save the DataFrame as a parquet file in blob
                file_name = f"{file_name.lower().split('.csv')[0]}.parquet"
                path_name = f"../data/input/{file_name}"
                raw_df.write_parquet(path_name, use_pyarrow=True, compression='zstd')

                file_client = self.landing_client.get_file_client(file_name)

                with open(file=path_name, mode="rb") as data:
                    file_client.upload_data(data, overwrite=True)
                logger_normal.info(f"{file_name} uploaded to adlsgen2")
                self.file_name = file_name
        except requests.exceptions.HTTPError as e:
            logger_normal.error(f"Failed to download {uri}. HTTP error occurred: {e}")
        except Exception as e:
            logger_normal.error(f"An error occurred processing {uri}: {e}")
            raise e

    def raw_to_bronze(self):
        """Append the data from the raw landing zone to the bronze layer."""
        try:
            storage_options_raw = {"account_name": ACCOUNT_NAME, "anon": False}
            storage_options_raw_delta = {"account_name": ACCOUNT_NAME, "use_azure_cli": "True"}
            logger_normal.info(f"Reading {self.file_name}")
            df = pl.read_parquet(
                source=f'abfss://{LANDING_ZONE_PATH}/{self.file_name}', storage_options=storage_options_raw
            )
            df = pl.read_parquet(source=f'../data/input/{self.file_name}')
            # df = df.with_columns(insertion_date=datetime.now())
            # df = df.with_columns([(pl.col("start_time").str.to_datetime().dt.strftime("%Y-%m").alias("monthdate"))])
            logger_normal.info(f"Converting {self.file_name} to delta")
            df.write_delta(
                target=f'abfss://{BRONZE_CONTAINER}/',
                mode='append',
                storage_options=storage_options_raw_delta
                # delta_write_options={"partition_by": ['monthdate']}
            )
            logger_normal.info("Optimizing by Z order for bronze table")
            bronze_df = DeltaTable(table_uri=f'abfs://{BRONZE_CONTAINER}/', storage_options=storage_options_raw_delta)
            bronze_df.optimize.z_order(['trip_id'])
        except Exception as e:
            logger_normal.error(f"Failed to conver to delta {self.file_name}")
            raise e

    def bronze_to_silver(self):
        """Merge the data incrementally and into the silver table."""
        try:
            storage_options_raw_delta = {"account_name": ACCOUNT_NAME, "use_azure_cli": "True"}

            # source
            bronze_df = pl.read_delta(source=f'abfss://{BRONZE_CONTAINER}/', storage_options=storage_options_raw_delta)
            # target
            silver_check = self._table_checker(container=SILVER_CONTAINER, options=storage_options_raw_delta)
            if silver_check:
                logger_normal.info("Merging new data into silver")
                silver_df = DeltaTable(
                    table_uri=f'abfss://{SILVER_CONTAINER}/', storage_options=storage_options_raw_delta
                )
                (
                    silver_df.merge(
                        source=bronze_df.to_arrow(),
                        predicate="s.trip_id = t.trip_id",
                        source_alias="s",
                        target_alias="t",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
                logger_normal.info("Optimizing by Z order for silver table")
                silver_df.optimize.z_order(['trip_id'])
                logger_normal.info(f'History of operations: {silver_df.get_add_actions().to_pandas()}')
            else:
                logger_normal.info("Because silver table is empty we save the first bronze file as silver")
                bronze_df.write_delta(
                    target=f'abfss://{SILVER_CONTAINER}/', mode='append', storage_options=storage_options_raw_delta
                )
        except Exception as e:
            logger_normal.error(f"Failed to merge {self.file_name}")
            raise e

    def silver_to_gold(self):
        """Aggregate the data in gold tables."""
        return True

    def _table_checker(self, container, options):
        """Internal method to check if the delta table exists."""
        try:
            delta_table = DeltaTable(table_uri=f"abfss://{container}/", storage_options=options)
            logger_normal.info(f"Delta table version is {delta_table.version()}")
            table_exist = True
            logger_normal.info(f"Delta Table Exists in {container}")
        except Exception as e:
            logger_normal.error(e)
            table_exist = False
        return table_exist


if __name__ == "__main__":
    t = time.time()
    etl_workflow = ETLPipeline()
    for uri in DOWNLOAD_URIS:
        try:
            etl_workflow.upload_to_landing(uri=uri)
            etl_workflow.raw_to_bronze()
            etl_workflow.bronze_to_silver()
        except Exception as e:
            logger_normal.error(e)
            logger_normal.info("Continuing for next file")
            pass
    logger_normal.info(f'Total runtime was {time.time()-t}')
