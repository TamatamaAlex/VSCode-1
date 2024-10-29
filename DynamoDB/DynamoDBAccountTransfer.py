import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def copy_table(source_account_profile, target_account_profile, source_table_name, target_table_name):
    source_account_session = boto3.Session(profile_name=source_account_profile)
    source_account_dynamodb = source_account_session.client('dynamodb')

    target_account_session = boto3.Session(profile_name=target_account_profile)
    target_account_dynamodb = target_account_session.client('dynamodb')

    dynamo_paginator = source_account_dynamodb.get_paginator('scan')
    dynamo_response = dynamo_paginator.paginate(
        TableName=source_table_name,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True
    )

    item_count = 0
    for page in dynamo_response:
        for item in page['Items']:
            try:
                response = target_account_dynamodb.put_item(
                    TableName=target_table_name,
                    Item=item
                )
                item_count += 1
                logger.info(f"Successfully put item: {item}")
                logger.debug(f"Put item response: {response}")
            except Exception as e:
                logger.error(f"Failed to put item: {item}, error: {e}")

    logger.info(f"Total items copied from {source_table_name} to {target_table_name}: {item_count}")

def copy_all_tables(source_account_profile, target_account_profile):
    source_account_session = boto3.Session(profile_name=source_account_profile)
    source_account_dynamodb = source_account_session.client('dynamodb')

    target_account_session = boto3.Session(profile_name=target_account_profile)
    target_account_dynamodb = target_account_session.client('dynamodb')

    # List all tables in the source account
    table_list = source_account_dynamodb.list_tables()
    for table_name in table_list['TableNames']:
        logger.info(f"Copying table: {table_name}")
        copy_table(source_account_profile, target_account_profile, table_name, table_name)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Copy all DynamoDB tables from one AWS account to another.')
    parser.add_argument('source_account_profile', type=str, help='Source AWS account profile name')
    parser.add_argument('target_account_profile', type=str, help='Target AWS account profile name')

    args = parser.parse_args()
    copy_all_tables(args.source_account_profile, args.target_account_profile)