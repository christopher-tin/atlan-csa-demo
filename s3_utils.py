import requests
import xmltodict

# List all objects in the S3 bucket
def list_s3_objects():
    """
    Fetches and parses the list of objects from a public AWS S3 bucket.
    Makes a GET request to the specified S3 bucket URL, parses the XML response,
    and returns a list of dictionaries containing metadata for each object in the bucket.
    Returns:
        list of dict: A list where each dictionary contains the following keys:
            - 'key' (str): The object's key (filename/path) in the bucket.
            - 'last_modified' (str): The timestamp when the object was last modified.
            - 'size' (str): The size of the object in bytes.
            - 'storage_class' (str): The storage class of the object.
            - 'etag' (str): The entity tag (ETag) of the object.
    Raises:
        requests.RequestException: If the HTTP request to the S3 bucket fails.
        xmltodict.expat.ExpatError: If the XML response cannot be parsed.
        KeyError: If the expected keys are not found in the parsed XML.
    """
    
    url = f"https://atlan-tech-challenge.s3.amazonaws.com"
    response = requests.get(url)
    parsed = xmltodict.parse(response.text)
    aws_s3_objects = []
    for content in parsed['ListBucketResult']['Contents']:
        result = {
            'key': content['Key'],
            'last_modified': content['LastModified'],
            'size': content['Size'],
            'storage_class': content['StorageClass'],
            'etag': content['ETag']
        }
        aws_s3_objects.append(result)
    return aws_s3_objects