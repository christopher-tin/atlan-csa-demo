from pyatlan.model.assets import Process
import logging

logger = logging.getLogger(__name__)

def create_lineage_postgres_to_s3(connection_qualified_name, table, s3_object, batch):
    """
    Creates a lineage process in Atlan representing data movement from a PostgreSQL table to an S3 object.
    Args:
        connection_qualified_name (str): The qualified name of the connection in Atlan.
        table: The source PostgreSQL table asset object.
        s3_object: The target S3 object asset object.
        batch: The batch instance used to interact with the asset management system.
    Returns:
        None
    Side Effects:
        - Creates and saves a lineage process asset in Atlan linking the specified table and S3 object.
        - Logs an informational message upon successful creation.
    Raises:
        Any exceptions raised by the Atlan client during asset creation or saving.
    """
    
    process = Process.creator(
        name=f"{table.name} - Postgres to S3",
        connection_qualified_name=connection_qualified_name,
        inputs=[table],
        outputs=[s3_object]
    )
    batch.add(process)
    logger.info(f"Created process lineage between postgres table {table.name} and s3 object {s3_object.name}.")


def create_lineage_s3_to_snowflake(connection_qualified_name, s3_object, table, batch):
    """
    Creates a lineage process in Atlan between an S3 object and a Snowflake table.
    This function establishes a process lineage in Atlan, representing the data flow from a specified S3 object to a Snowflake table. It creates a process asset with the given connection qualified name, using the S3 object as input and the Snowflake table as output, and saves it via the provided Atlan client.
    Args:
        connection_qualified_name (str): The qualified name of the connection in Atlan.
        s3_object: The S3 object asset to be used as the input in the lineage process.
        table: The Snowflake table asset to be used as the output in the lineage process.
        batch: The batch instance used to interact with the asset management system.
    Returns:
        None
    Logs:
        Info message indicating the creation of the process lineage between the S3 object and the Snowflake table.
    """
    
    process = Process.creator(
        name=f"{table.name} - S3 to Snowflake",
        connection_qualified_name=connection_qualified_name,
        inputs=[s3_object],
        outputs=[table]
    )
    batch.add(process)
    logger.info(f"Created process lineage between s3 object {s3_object.name} and snowflake table {table.name}.")
