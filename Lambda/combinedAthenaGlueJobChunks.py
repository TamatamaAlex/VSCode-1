import boto3
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

# AWS settings
region = "ap-northeast-1"
source_database_name = "bcart-export-for-data-lake"
target_database_name = "bcart-export-for-data-lake-test-combine"
output_bucket = "s3://bcart-export-for-data-lake-athena-results/"
combined_table_names = {
    "order": "combined_orders_test2",
    "order_totals": "combined_order_totals_test2",
    "order_products": "combined_order_products_test2",
    "conf": "combined_conf_test2"
}
chunk_size = 50
max_concurrent_queries = 5

# Initialize Glue and Athena clients
glue_client = boto3.client('glue', region_name=region)
athena_client = boto3.client('athena', region_name=region)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Semaphore to limit concurrency
semaphore = Semaphore(max_concurrent_queries)

def get_table_names(database_name):
    """Fetch table names from Glue Data Catalog."""
    tables = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            tables.append(table['Name'])
    return tables

def chunk_table_names_with_limit(table_names, chunk_size, suffix, columns):
    """Split table names into chunks, ensuring query length stays under Athena limits."""
    chunk = []
    query_size = 0
    for table_name in table_names:
        if table_name.endswith(suffix):
            table_query = f"""
            SELECT '{table_name}' AS source, {columns}
            FROM "{source_database_name}"."{table_name}"
            """
            if query_size + len(table_query) > 200000:
                yield chunk
                chunk = []
                query_size = 0
            chunk.append(table_name)
            query_size += len(table_query)
    if chunk:
        yield chunk

def generate_union_query(table_names_chunk, suffix, columns):
    """Generate a UNION ALL query for a chunk of tables."""
    union_queries = []
    for table_name in table_names_chunk:
        if table_name.endswith(suffix):
            union_queries.append(f"""
            SELECT '{table_name}' AS source, {columns}
            FROM "{source_database_name}"."{table_name}"
            """)
    return " UNION ALL ".join(union_queries)

def create_table_query(combined_table_name, union_query, is_first_chunk):
    """Generate a CREATE TABLE or INSERT INTO query for the combined table."""
    if is_first_chunk:
        return f"""
        CREATE TABLE "{target_database_name}"."{combined_table_name}" AS
        {union_query}
        """
    else:
        return f"""
        INSERT INTO "{target_database_name}"."{combined_table_name}"
        {union_query}
        """

def execute_query(query, database, output_location):
    """Execute an Athena query and return the execution ID."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

def check_query_status(execution_id):
    """Check the status of an Athena query execution."""
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status != 'SUCCEEDED':
                failure_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                logger.error(f"Query {execution_id} failed: {failure_reason}")
            return status
        time.sleep(5)

def run_combined_query(table_name, query):
    """Run a combined query with retries and error handling."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Executing query for {table_name} (attempt {attempt + 1})")
            execution_id = execute_query(query, target_database_name, output_bucket)
            status = check_query_status(execution_id)
            if status == 'SUCCEEDED':
                logger.info(f"Query succeeded for {table_name}. Execution ID: {execution_id}")
                return
            else:
                logger.error(f"Query failed for {table_name}. Execution ID: {execution_id}")
        except Exception as e:
            logger.error(f"Error executing query for {table_name}: {e}")
        time.sleep(10)
    logger.error(f"Query failed for {table_name} after {max_retries} attempts")

columns_definitions = {
    "order": """order_id, order_code, customer_id, ext_id, parent_id, salesman_account,
                order_name, order_comp_name, order_comp_depa, order_tel, order_mobile_phone, 
                order_email, order_stage, total_price, payment_date, tax, tax_rate, tax_settings, 
                qualified_invoice, cod_cost, order_zip, order_pref, order_address01, order_address02, 
                order_address03, payment, payment_option, shipping_cost, final_price, use_point, 
                get_point, text, info, order_memo, order_time, api_status, payment_fin, status, af_id, 
                enquete1, enquete2, enquete3, enquete4, enquete5, relation_id, customer_custom1, 
                customer_custom2, customer_custom3, partition_0""",
    "order_totals": "order_id, tax_rate, total, tax, total_incl_tax, partition_0",
    "order_products": """order_pro_id, order_id, order_logistics_id, product_id, main_no, product_no, 
                jan_code, location_no, product_name, set_id, set_name, unit_price, set_quantity, set_unit, 
                order_pro_count, tax_rate, tax_type_id, tax_incl, item_type, shipping_size, set_custom1, 
                set_custom2, set_custom3, pro_custom1, pro_custom2, pro_custom3, partition_0""",
    "conf": "id, name, value, serialize, name_jp, memo, partition_0"
}

try:
    table_names = get_table_names(source_database_name)
    with ThreadPoolExecutor(max_workers=max_concurrent_queries) as executor:
        futures = []
        for suffix, combined_table_name in combined_table_names.items():
            columns = columns_definitions[suffix]
            is_first_chunk = True
            for table_names_chunk in chunk_table_names_with_limit(table_names, chunk_size, suffix, columns):
                union_query = generate_union_query(table_names_chunk, suffix, columns)
                if not union_query:
                    continue
                create_or_insert_query = create_table_query(combined_table_name, union_query, is_first_chunk)
                logger.debug(f"Executing query for table {combined_table_name}: {create_or_insert_query}")
                if is_first_chunk:
                    # Execute the table creation query first
                    run_combined_query(combined_table_name, create_or_insert_query)
                    is_first_chunk = False
                else:
                    semaphore.acquire()
                    future = executor.submit(run_combined_query, combined_table_name, create_or_insert_query)
                    future.add_done_callback(lambda p: semaphore.release())
                    futures.append(future)
        for future in as_completed(futures):
            future.result()
except Exception as e:
    logger.error(f"Error occurred: {str(e)}")