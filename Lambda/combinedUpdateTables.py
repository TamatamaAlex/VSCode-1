import boto3
import time

# AWS settings
region = "ap-northeast-1"
database_name = "auroratos3exporttest"
output_bucket = "s3://auroratos3autoexport/"
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

def create_update_table_query(combined_table_name, union_query):
    """Generate an INSERT INTO SELECT query for updating the combined table."""
    query = f"""
    INSERT INTO "{database_name}"."{combined_table_name}"
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

# Column definitions
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

# Example usage
try:
    table_names = get_table_names(database_name)
    for suffix, combined_table_name in combined_table_names.items():
        columns = columns_definitions[suffix]
        union_query = generate_union_query(table_names, suffix, columns)
        if not union_query:
            print(f"No valid columns found for {suffix}. Skipping update of {combined_table_name}.")
            continue
        update_table_query = create_update_table_query(combined_table_name, union_query)
        print(f"Executing query for {combined_table_name}: {update_table_query}")
        query_execution_id = execute_athena_query(update_table_query, output_bucket)
        status = check_query_status(query_execution_id)
        if status == 'SUCCEEDED':
            print(f"Update table query for {combined_table_name} succeeded. Execution ID: {query_execution_id}")
        else:
            print(f"Update table query for {combined_table_name} failed. Execution ID: {query_execution_id}")
except Exception as e:
    print(f"Error occurred: {str(e)}")