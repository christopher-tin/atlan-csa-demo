from pyatlan.client.atlan import AtlanClient
from config import ATLAN_BASE_URL, ATLAN_API_KEY

client = AtlanClient(
    base_url=ATLAN_BASE_URL,
    api_key=ATLAN_API_KEY,
)

def get_atlan_client():
    """
    Creates and returns an instance of AtlanClient using the configured base URL and API key.
    Returns:
        AtlanClient: An authenticated client for interacting with the Atlan API.
    """
    
    return client