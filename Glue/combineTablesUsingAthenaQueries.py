#Uses athena SQL queries to combine tables instead of using a python script.
import boto3
import time

# AWS settings
region = "ap-northeast-1"
database_name = "name of glue DB"
output_bucket = "s3://outputbucketname/"
combined_table_names = {
    "order": "combined_orders_test",
    "order_totals": "combined_order_totals_test",
    "order_products": "combined_order_products_test",
    "conf": "combined_conf_test"
}

# Initialize Glue and Athena clients
glue_client = boto3.client('glue', region_name=region)
athena_client = boto3.client('athena', region_name=region)

def get_table_names(database_name):
    """Fetch table names from Glue Data Catalog."""
    tables = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            tables.append(table['Name'])
    return tables

def generate_union_query(table_names, suffix, columns):
    """Generate a UNION ALL query for the specified table suffix."""
    union_queries = []
    for table_name in table_names:
        if table_name.endswith(suffix):
            union_queries.append(f"""
            SELECT '{table_name}' AS source, {columns}
            FROM "{database_name}"."{table_name}"
            """)
    return " UNION ALL ".join(union_queries)

def create_combined_table_query(combined_table_name, union_query):
    """Generate a CREATE TABLE AS SELECT query for the combined table."""
    query = f"""
    CREATE TABLE "{database_name}"."{combined_table_name}" AS
    {union_query}
    """
    return query

def execute_athena_query(query, output_bucket):
    """Execute a query in Athena."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': output_bucket}
    )
    return response['QueryExecutionId']

def check_query_status(query_execution_id):
    """Check the status of the Athena query."""
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status
        time.sleep(5)

#Column definitions. Insert your own columns here.
columns_definitions = {
    "order": "order_id, customer_id, order_date"
}

# Example usage
try:
    table_names = get_table_names(database_name)
    for suffix, combined_table_name in combined_table_names.items():
        columns = columns_definitions[suffix]
        union_query = generate_union_query(table_names, suffix, columns)
        if not union_query:
            print(f"No valid columns found for {suffix}. Skipping creation of {combined_table_name}.")
            continue
        create_table_query = create_combined_table_query(combined_table_name, union_query)
        print(f"Executing query for {combined_table_name}: {create_table_query}")
        query_execution_id = execute_athena_query(create_table_query, output_bucket)
        status = check_query_status(query_execution_id)
        if status == 'SUCCEEDED':
            print(f"Create table query for {combined_table_name} succeeded. Execution ID: {query_execution_id}")
        else:
            print(f"Create table query for {combined_table_name} failed. Execution ID: {query_execution_id}")
except Exception as e:
    print(f"Error occurred: {str(e)}")