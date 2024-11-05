#BROKEN CODE DON'T USE
#This script is not working, but it may be useful for the future.
#Rather than just copying and writing the items to the DynamoDB in the target account, this script
#was an attempt to carry the indexes and items over to the target account, while also creating the DynamoDB tables in the target account.

import boto3
import time
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Source account session using profile1
source_session = boto3.Session(profile_name='profile1')

# Target account session using profile2
target_session = boto3.Session(profile_name='profile2')

# Initialize clients
source_dynamodb = source_session.client('dynamodb')
source_s3 = source_session.client('s3')
target_s3 = target_session.client('s3')
target_dynamodb = target_session.client('dynamodb')

# Export DynamoDB table to S3 in source account
def export_dynamodb_to_s3(source_table_name, source_bucket_name):
    try:
        print(f"Exporting table {source_table_name} to bucket {source_bucket_name} in folder {source_table_name}")
        response = source_dynamodb.export_table_to_point_in_time(
            TableArn=f'arn:aws:dynamodb:ap-northeast-1:<accountIDREPLACE>:table/{source_table_name}',
# Initialize clients
            ExportFormat='DYNAMODB_JSON'
        )
        return response['ExportDescription']['ExportArn']
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Check the status of the export operation
def check_export_status(export_arn):
    try:
        while True:
            response = source_dynamodb.describe_export(ExportArn=export_arn)
            status = response['ExportDescription']['ExportStatus']
            if status == 'COMPLETED':
                print(f"Export completed: {export_arn}")
                return True
            elif status == 'FAILED':
                print(f"Export failed: {export_arn}")
                return False
            else:
                print(f"Export in progress: {export_arn}")
                time.sleep(30)  # Wait for 30 seconds before checking again
    except Exception as e:
        print(f"An error occurred while checking export status: {e}")
        return False

# Copy data from source S3 bucket to target S3 bucket
def copy_s3_bucket(source_bucket_name, target_bucket_name, source_table_name):
    try:
        print(f"Copying data from bucket {source_bucket_name}/{source_table_name} to bucket {target_bucket_name}/{source_table_name}")
        source_objects = source_s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_table_name)
        if 'Contents' not in source_objects:
            print(f"No objects found in {source_bucket_name}/{source_table_name}")
            return False
        for obj in source_objects['Contents']:
            copy_source = {'Bucket': source_bucket_name, 'Key': obj['Key']}
            target_s3.copy_object(CopySource=copy_source, Bucket=target_bucket_name, Key=obj['Key'])
        return True
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# Fetch table schema from source DynamoDB table
def get_table_schema(table_name):
    try:
        response = source_dynamodb.describe_table(TableName=table_name)
        table_schema = response['Table']
        
        schema = {
            'AttributeDefinitions': table_schema['AttributeDefinitions'],
            'KeySchema': table_schema['KeySchema'],
            'BillingMode': 'PAY_PER_REQUEST'  # Always use on-demand billing mode
        }
        
        if 'GlobalSecondaryIndexes' in table_schema:
            schema['GlobalSecondaryIndexes'] = [
                {
                    'IndexName': gsi['IndexName'],
                    'KeySchema': gsi['KeySchema'],
                    'Projection': gsi['Projection']
                }
                for gsi in table_schema['GlobalSecondaryIndexes']
            ]
        
        if 'LocalSecondaryIndexes' in table_schema:
            schema['LocalSecondaryIndexes'] = [
                {
                    'IndexName': lsi['IndexName'],
                    'KeySchema': lsi['KeySchema'],
                    'Projection': lsi['Projection']
                }
                for lsi in table_schema['LocalSecondaryIndexes']
            ]
        
        print(f"Table {table_name} will be created with PAY_PER_REQUEST billing mode.")
        
        return schema
    except Exception as e:
        print(f"An error occurred while fetching table schema for {table_name}: {e}")
        return None

# Check the status of the import operation
def check_import_status(import_arn):
    try:
        while True:
            response = target_dynamodb.describe_import(ImportArn=import_arn)
            status = response['ImportTableDescription']['ImportStatus']
            if status == 'COMPLETED':
                print(f"Import completed: {import_arn}")
                return True
            elif status == 'FAILED':
                print(f"Import failed: {import_arn}")
                return False
            else:
                print(f"Import in progress: {import_arn}")
                time.sleep(30)  # Wait for 30 seconds before checking again
    except Exception as e:
        print(f"An error occurred while checking import status: {e}")
        return False

# Import data from S3 to DynamoDB in target account
def import_s3_to_dynamodb(target_table_name, target_bucket_name):
    try:
        print(f"Importing data from bucket {target_bucket_name}/{target_table_name} to table {target_table_name}")
        table_schema = get_table_schema(target_table_name)
        if not table_schema:
            print(f"Failed to fetch schema for table {target_table_name}")
            return None

        table_creation_params = {
            'TableName': target_table_name,
            'AttributeDefinitions': table_schema['AttributeDefinitions'],
            'KeySchema': table_schema['KeySchema'],
            'BillingMode': 'PAY_PER_REQUEST'  # Always use on-demand billing mode
        }

        if 'GlobalSecondaryIndexes' in table_schema:
            table_creation_params['GlobalSecondaryIndexes'] = table_schema['GlobalSecondaryIndexes']
        
        if 'LocalSecondaryIndexes' in table_schema:
            table_creation_params['LocalSecondaryIndexes'] = table_schema['LocalSecondaryIndexes']

        response = target_dynamodb.import_table(
            ClientToken='import-' + target_table_name,
            S3BucketSource={
                'S3Bucket': target_bucket_name,
                'S3KeyPrefix': target_table_name
            },
            InputFormat='DYNAMODB_JSON',
            InputCompressionType='GZIP',  # Specify the compression type
            TableCreationParameters=table_creation_params
        )
        print(f"Import response: {response}")
        return response['ImportTableDescription']['ImportArn']
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# List of source table names
source_table_names = [
    "list all table names here"
]

source_bucket_name = 'bucket name here'
target_bucket_name = 'bucket name here'

# Iterate over each table and perform export, copy, and import
for table_name in source_table_names:
    export_arn = export_dynamodb_to_s3(table_name, source_bucket_name)
    if export_arn:
        print(f"Export started for {table_name}: {export_arn}")
        if check_export_status(export_arn):
            if copy_s3_bucket(source_bucket_name, target_bucket_name, table_name):
                import_arn = import_s3_to_dynamodb(table_name, target_bucket_name)
                if import_arn:
                    print(f"Import started for {table_name}: {import_arn}")
                    if check_import_status(import_arn):
                        print(f"Import completed for {table_name}")
