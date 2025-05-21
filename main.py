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
    connection_qualified_name = get_or_create_s3_connection()
    logger.info(f"S3 connection qualified_name: {connection_qualified_name}")
    bucket_qualified_name = get_or_create_s3_bucket(connection_qualified_name)
    logger.info(f"S3 bucket qualified_name: {bucket_qualified_name}")
    
    try:
        # Creates all S3 objects assets found in the bucket
        aws_s3_objects = list_s3_objects()
        for s3_object in aws_s3_objects:
            file_name = s3_object['key']
            create_s3_object(connection_qualified_name, bucket_qualified_name, s3_object, batch)
        batch.flush()
        
        # Creates lineage between Postgres table and S3 object and between S3 object and Snowflake table
        for s3_object in aws_s3_objects:
            table_name = s3_object['key'].split(".")[0]
            file_name = s3_object['key']
            create_s3_object(connection_qualified_name, bucket_qualified_name, s3_object, batch)
            postgres_table = get_postgres_table(table_name)
            atlan_s3_obj = get_atlan_s3_object(bucket_qualified_name, file_name)
            snowflake_table = get_snowflake_table(table_name)
            create_lineage_postgres_to_s3(connection_qualified_name, postgres_table, atlan_s3_obj, batch)
            create_lineage_s3_to_snowflake(connection_qualified_name, atlan_s3_obj, snowflake_table, batch)
        update_bucket_object_count(bucket_qualified_name, len(aws_s3_objects))
        batch.flush()
        logger.info("Batch processing completed.")
    except Exception as e:
        logger.error(f"Error processing object {s3_object['key']}: {e}. Please check if the table exists in Postgres or Snowflake.")

if __name__ == "__main__":
    main()