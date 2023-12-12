import asyncio

from azure.eventhub.aio import EventHubConsumerClient
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.serializer.avroserializer import AvroSerializer
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore,
)
from azure.identity.aio import DefaultAzureCredential
from deltalake import DeltaTable
from dotenv import load_dotenv

load_dotenv('../.env')
# Set the logging level for all azure-* libraries
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('azure')
logger.setLevel(logging.ERROR)
logger_normal = logging.getLogger(__name__)

LANDING_ZONE_PATH = os.getenv('LANDING_ZONE_PATH')
ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')
BRONZE_CONTAINER = os.getenv('APPEND_LAYER')
SILVER_CONTAINER = os.getenv('HISTORICAL_PATH')
GOLD_CONTAINER = os.getenv('DW_PATH')
EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.getenv("EVENT_HUB_FULLY_QUALIFIED_NAMESPACE")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
GROUP_NAME = os.getenv("GROUP_NAME")

schema_registry_client = SchemaRegistryClient(EVENT_HUB_FULLY_QUALIFIED_NAMESPACE, token_credential)
avro_serializer = AvroSerializer(client=schema_registry_client, group_name=GROUP_NAME)

async def on_event(partition_context, event):
    # Print the event data.
    bytes_payload = b"".join(b for b in event.body)
    deserialized_data = avro_serializer.deserialize(bytes_payload)
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            deserialized_data, partition_context.partition_id
        )
    )

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)


async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore(
        blob_account_url=ACCOUNT_NAME,
        container_name=BRONZE_CONTAINER,
        credential=credential,
    )

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient(
        fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        consumer_group="$Default",
        checkpoint_store=checkpoint_store,
        credential=credential,
    )
    async with client, avro_serializer:
        # Call the receive method. Read from the beginning of the partition
        # (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")
        ### add polars to delta lake here?

    # Close credential when no longer needed.
    await credential.close()

if __name__ == "__main__":
    # Run the main method.
    asyncio.run(main())
