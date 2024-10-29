import boto3
import os

def lambda_handler(event, context):
    rds_client = boto3.client('rds')
    
    # Parameters
    db_identifier = os.environ['bcart-develop']
    s3_bucket_name = os.environ['auroratos3autoexport']
    iam_role_arn = os.environ['IAM_ROLE_ARN']
    export_task_identifier = f"export-task-{db_identifier}"
    
    try:
        response = rds_client.start_export_task(
            ExportTaskIdentifier=export_task_identifier,
            SourceArn=f"arn:aws:rds:us-west-2:123456789012:db:{db_identifier}",
            S3BucketName=s3_bucket_name,
            IamRoleArn=iam_role_arn,
            KmsKeyId='arn:aws:kms:us-west-2:123456789012:key/your-kms-key-id'
        )
        return {
            'statusCode': 200,
            'body': f"Export task started: {response['ExportTaskIdentifier']}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }