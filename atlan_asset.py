from pyatlan.model.assets import S3Bucket, S3Object, Table, Connection
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.errors import NotFoundError
import logging
from config import BUCKET_ARN_CUSTOM, BUCKET_NAME
from atlan_client import get_atlan_client

logger = logging.getLogger(__name__)

client = get_atlan_client()

def get_or_create_s3_connection():
    """
    Retrieve the qualified name of an existing AWS S3 connection or create a new one if it does not exist.
    This function attempts to find an S3 connection with a specific name and connector type.
    If the connection is not found, it creates a new S3 connection with the given parameters.
    Args:
        client: The client object used to interact with the asset management system. It must provide
            access to role caching and asset management methods.
    Returns:
        str: The qualified name of the existing or newly created S3 connection.
    Raises:
        Any exceptions raised by the underlying client methods except NotFoundError, which is handled internally.
    """
    
    try:
        admin_role_guid = client.role_cache.get_id_for_name("$admin")
        response = client.asset.find_connections_by_name(name="aws-s3-connection-ct", connector_type=AtlanConnectorType.S3)
        return response[0].qualified_name
    except NotFoundError:
        logger.info("Connection not found, creating a new one.")
        connection = Connection.creator(
            name="aws-s3-connection-ct",
            connector_type=AtlanConnectorType.S3,
            admin_roles=[admin_role_guid]
        )
        response = client.asset.save(connection)
        return response.assets_created(asset_type=Connection)[0].qualified_name


def get_or_create_s3_bucket(connection_qualified_name):
    """
    Retrieves the qualified name of an S3 bucket asset from Atlan if it exists, or creates it if it does not.
    This function searches for an S3 bucket asset in Atlan with a specific name. If the asset does not exist,
    it creates a new S3 bucket asset using the provided connection qualified name and a custom AWS ARN.
    If multiple assets with the same name are found, an exception is raised.
    Args:
        client: The Atlan client instance used to interact with the Atlan API.
        connection_qualified_name (str): The qualified name of the connection to associate with the S3 bucket.
    Returns:
        str: The qualified name of the existing or newly created S3 bucket asset.
    Raises:
        Exception: If multiple S3 bucket assets with the same name are found in Atlan.
    """
    
    request = (
        FluentSearch()
        .where(FluentSearch.asset_type(S3Bucket))
        .where(FluentSearch.active_assets())
        .where(S3Bucket.NAME.eq(BUCKET_NAME))
    ).to_request()
    search_results = [b for b in client.asset.search(request)]
    if len(search_results) == 0:
        s3bucket = S3Bucket.creator(
            name=BUCKET_NAME,
            connection_qualified_name=connection_qualified_name,
            aws_arn=BUCKET_ARN_CUSTOM
        )
        response = client.asset.save(s3bucket)
        qualified_name = response.assets_created(asset_type=S3Bucket)[0].qualified_name
        logger.info(f"S3 Bucket not found in Atlan. Creating the asseting.")
        return qualified_name
    elif len(search_results) > 1:
        raise Exception("Multiple S3 buckets found in Atlan with the same name.")
    else:
        qualified_name = search_results[0].qualified_name
        return qualified_name


def create_s3_object(connection_qualified_name, bucket_qualified_name, s3_object, batch):
    """
    Creates and saves an S3 object asset using the provided client and metadata.
    Args:
        connection_qualified_name (str): The qualified name of the S3 connection.
        bucket_qualified_name (str): The qualified name of the S3 bucket.
        s3_object (dict): A dictionary containing S3 object metadata with keys:
            - 'key' (str): The object key (name) in the bucket.
            - 'etag' (str): The entity tag (ETag) of the S3 object.
            - 'last_modified' (str or datetime): The last modified timestamp of the object.
            - 'size' (int): The size of the object in bytes.
            - 'storage_class' (str): The storage class of the S3 object.
        batch: The batch instance used to interact with the asset management system.
    Returns:
        None
    """
    
    if get_atlan_s3_object(bucket_qualified_name, s3_object['key']) is not None:
        logger.debug(f"S3 object {s3_object['key']} already exists in Atlan.")
        return
    updater = S3Object.creator(
        name=s3_object['key'],
        connection_qualified_name=connection_qualified_name,
        aws_arn=f"{BUCKET_ARN_CUSTOM}/{s3_object['key']}",
        s3_bucket_name=BUCKET_NAME,
        s3_bucket_qualified_name=bucket_qualified_name,
    )
    updater.s3_e_tag = s3_object['etag']
    updater.s3_object_last_modified_time = s3_object['last_modified']
    updater.s3_object_size = s3_object['size']
    updater.s3_object_storage_class = s3_object['storage_class']
    batch.add(updater)


def update_bucket_object_count(bucket_qualified_name, count):
    """
    Updates the object count for a specified S3 bucket asset.
    Args:
        client: The client instance used to interact with the asset management system.
        bucket_qualified_name (str): The qualified name of the S3 bucket to update.
        count (int): The new object count to set for the bucket.
    Returns:
        None
    Logs:
        Logs an informational message indicating the bucket has been updated with the specified object count.
    """
    
    updater = S3Bucket.updater(
        qualified_name=bucket_qualified_name,
        name=BUCKET_NAME,
    )
    updater.s3_object_count = count
    client.asset.save(updater)
    logger.info(f"Bucket updated with {count} objects.")

# Asset fetchers (with caching)
postgres_tables = []
snowflake_tables = []
atlan_s3_objects = []

def get_postgres_table(table_name):
    """
    Retrieve a Postgres table asset by name from Atlan, utilizing a cached list for efficiency.
    If the cache of Postgres tables (`postgres_tables`) is empty, this function fetches all active Postgres table assets
    from Atlan using the provided client and populates the cache. It then searches for a table with the specified name
    in the cached list and returns it if found.
    Args:
        client: The Atlan client instance used to perform asset searches.
        table_name (str): The name of the Postgres table to retrieve.
    Returns:
        Table: The Postgres table asset object matching the specified name.
    Raises:
        Exception: If a table with the given name is not found in the cached list of Postgres tables.
    """

    if len(postgres_tables) == 0:
        logger.info("Fetching postgres tables from Atlan and populating cache.")
        request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Table))
            .where(FluentSearch.active_assets())
            .where(Table.CONNECTION_NAME.eq("postgres-ct"))
        ).to_request()
        for result in client.asset.search(request):
            postgres_tables.append(result)
    for table in postgres_tables:
        if table_name == table.name:
            return table
    raise Exception(f"Table {table_name} not found in postgres tables.")

def get_snowflake_table(table_name):
    """
    Retrieve a Snowflake table asset by name, utilizing a cached list of tables.
    If the cache of Snowflake tables is empty, this function fetches all active Snowflake tables
    from Atlan using the provided client and populates the cache. It then searches for a table
    with the specified name in the cache and returns it if found.
    Args:
        client: The Atlan client instance used to perform the asset search.
        table_name (str): The name of the Snowflake table to retrieve.
    Returns:
        Table: The matching Snowflake table asset.
    Raises:
        Exception: If the specified table name is not found among the Snowflake tables.
    """
    
    if len(snowflake_tables) == 0:
        logger.info("Fetching snowflake tables from Atlan and populating cache.")
        request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Table))
            .where(FluentSearch.active_assets())
            .where(Table.CONNECTION_NAME.eq("snowflake-ct"))
        ).to_request()
        for result in client.asset.search(request):
            snowflake_tables.append(result)
    for table in snowflake_tables:
        if table_name == table.name:
            return table
    raise Exception(f"Table {table_name} not found in snowflake tables.")

def get_atlan_s3_object(bucket_qualified_name, s3_object_name):
    """
    Retrieve an S3 object from Atlan by its name and bucket qualified name.

    This function checks a local cache of S3 objects (`atlan_s3_objects`). If the cache is empty,
    it fetches all S3 objects from Atlan for the specified bucket and populates the cache.
    It then searches for an S3 object with the given name in the cache and returns it if found.

    Args:
        client: The Atlan client instance used to perform the asset search.
        bucket_qualified_name (str): The qualified name of the S3 bucket to search within.
        s3_object_name (str): The name of the S3 object to retrieve.

    Returns:
        S3Object: The matching S3 object from Atlan.

    Raises:
        Exception: If the S3 object with the specified name is not found in the cache.
    """
    if len(atlan_s3_objects) == 0:
        logger.info("Fetching s3 objects from Atlan and populating cache.")
        request = (
            FluentSearch()
            .where(FluentSearch.asset_type(S3Object))
            .where(FluentSearch.active_assets())
            .where(S3Object.S3BUCKET_QUALIFIED_NAME.eq(bucket_qualified_name))
        ).to_request()
        for result in client.asset.search(request):
            atlan_s3_objects.append(result)
    for s3_object in atlan_s3_objects:
        if s3_object_name == s3_object.name:
            return s3_object
    raise Exception(f"S3 object {s3_object_name} not found in atlan s3 objects.")
