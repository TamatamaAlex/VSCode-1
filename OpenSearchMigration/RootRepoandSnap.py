import boto3
import requests
from requests_aws4auth import AWS4Auth

host = 'your domain endpoint here'  # domain endpoint
region = 'ap-northeast-1'  # e.g., us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Register repository
path = '/_snapshot/insertname'  # the OpenSearch API endpoint for repository creation
url = host + path

payload = {
    "type": "s3",
    "settings": {
        "bucket": "bucketname",
        "base_path": "my/snapshot/directory",
        "region": "ap-northeast-1",
        "role_arn": "arn:aws:iam::account number:role/rolename"
        }
}

headers = {"Content-Type": "application/json"}

# Register the snapshot repository
r = requests.put(url, auth=awsauth, json=payload, headers=headers)
print(r.status_code)
print(r.text)

# Take snapshot
snapshot_name = "my-snapshot-001"  # specify a name for the snapshot
path = f'/_snapshot/bruuroottobruudev/{snapshot_name}'
url = host + path

# Perform the snapshot request without a JSON body
r = requests.put(url, auth=awsauth)

print(r.status_code)
print(r.text)



# # Delete index

# path = 'my-index'
# url = host + path
#
# r = requests.delete(url, auth=awsauth)
#
# print(r.text)
#
# # Restore snapshot (all indexes except Dashboards and fine-grained access control)
#
# path = '/_snapshot/my-snapshot-repo-name/my-snapshot/_restore'
# url = host + path
#
# payload = {
#   "indices": "-.kibana*,-.opendistro_security,-.opendistro-*",
#   "include_global_state": False
# }
#
# headers = {"Content-Type": "application/json"}
#
# r = requests.post(url, auth=awsauth, json=payload, headers=headers)
#
# print(r.text)
# 
# # Restore snapshot (one index)
#
# path = '/_snapshot/my-snapshot-repo-name/my-snapshot/_restore'
# url = host + path
#
# payload = {"indices": "my-index"}
#
# headers = {"Content-Type": "application/json"}
#
# r = requests.post(url, auth=awsauth, json=payload, headers=headers)
#
# print(r.text)