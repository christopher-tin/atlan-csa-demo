import os
from dotenv import load_dotenv; load_dotenv()

# Configuration and secrets
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY")
OPENAPI_API_KEY = os.getenv("OPENAPI_API_KEY")
ATLAN_BASE_URL = "https://tech-challenge.atlan.com"
BUCKET_ARN_CUSTOM = "arn:aws:s3:::atlan-tech-challenge-ct"
BUCKET_NAME = "atlan-tech-challenge-ct"