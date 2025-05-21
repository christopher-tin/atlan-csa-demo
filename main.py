import logging
from atlan_asset import (
    get_or_create_s3_connection, get_or_create_s3_bucket, create_s3_object,
    update_bucket_object_count, get_postgres_table, get_snowflake_table, get_atlan_s3_object
)
from atlan_client import get_atlan_client
from pyatlan.client.asset import Batch
from s3_utils import list_s3_objects
from atlan_lineage import create_lineage_postgres_to_s3, create_lineage_s3_to_snowflake

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def main():
    client = get_atlan_client()
    batch = Batch(
        client=client,
        max_size=50,
        update_only=False
    )
    connection_qualified_name = get_or_create_s3_connection(client)
    logger.info(f"S3 connection qualified_name: {connection_qualified_name}")
    bucket_qualified_name = get_or_create_s3_bucket(client, connection_qualified_name)
    logger.info(f"S3 bucket qualified_name: {bucket_qualified_name}")
    
    try:
        aws_s3_objects = list_s3_objects()
        for s3_object in aws_s3_objects:
            table_name = s3_object['key'].split(".")[0]
            file_name = s3_object['key']
            logger.info(f"Processing object: {s3_object['key']}")
            create_s3_object(connection_qualified_name, bucket_qualified_name, s3_object, batch)
            postgres_table = get_postgres_table(client, table_name)
            atlan_s3_obj = get_atlan_s3_object(client, bucket_qualified_name, file_name)
            snowflake_table = get_snowflake_table(client, table_name)
            create_lineage_postgres_to_s3(connection_qualified_name, postgres_table, atlan_s3_obj, batch)
            create_lineage_s3_to_snowflake(connection_qualified_name, atlan_s3_obj, snowflake_table, batch)
        update_bucket_object_count(client, bucket_qualified_name, len(aws_s3_objects))
        batch.flush()
        logger.info("Batch processing completed.")
    except Exception as e:
        logger.error(f"Error processing object {s3_object['key']}: {e}. Please check if the table exists in Postgres or Snowflake.")

if __name__ == "__main__":
    main()