import boto3
import requests
from requests_aws4auth import AWS4Auth

host = 'your domain endpoint'  # domain endpoint
region = 'ap-northeast-1'  # e.g., us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Register repository
path = '/_snapshot/bruuroottobruudev'  # the OpenSearch API endpoint for repository creation
url = host + path

payload = {
    "type": "s3",
    "settings": {
        "bucket": "opensearch-from-root",
        "base_path": "my/snapshot/directory",
        "region": "ap-northeast-1",
        "role_arn": "arn:aws:iam::accountnumber:role/rolename"
                                                }
                }

headers = {"Content-Type": "application/json"}

# Register the snapshot repository
r = requests.put(url, auth=awsauth, json=payload, headers=headers)
print(r.status_code)
print(r.text)