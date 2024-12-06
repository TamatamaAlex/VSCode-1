import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame


# AWS settings
region = "ap-northeast-1"
source_database_name = "auroratos3exporttest"  # Source database
target_database_name = "testdbathena"  # Target database for combined tables
output_bucket = "s3://auroratos3gluetest/" 
combined_table_names = {
    "order": "combined_orders_test",
    "order_totals": "combined_order_totals_test",
    "order_products": "combined_order_products_test",
    "conf": "combined_conf_test"
}
max_concurrent_queries = 10  # Increase concurrency to handle more tables at once

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('glue_job', {})

# Semaphore to limit concurrency
semaphore = Semaphore(max_concurrent_queries)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_names(database_name):
    """Fetch table names from Glue Data Catalog."""
    glue_client = boto3.client('glue', region_name=region)
    tables = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page['TableList']:
            tables.append(table['Name'])
    return tables

def read_table_to_dynamic_frame(table_name, database_name):
    """Read a table from the Glue Catalog into a DynamicFrame and add a source column."""
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name, 
        table_name=table_name
    )
    # Convert DynamicFrame to Spark DataFrame
    spark_df = dynamic_frame.toDF()
    # Add a new column with the source table name
    spark_df = spark_df.withColumn("source_table", F.lit(table_name))
    # Explicitly cast the "id" column to IntegerType if it exists
    if "id" in spark_df.columns:
        spark_df = spark_df.withColumn("id", spark_df["id"].cast(IntegerType()))
    # Convert back to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_with_source")
    return dynamic_frame

def combine_dynamic_frames(dynamic_frames):
    """Combine a list of DynamicFrames into one using union."""
    combined_frame = dynamic_frames[0]
    for frame in dynamic_frames[1:]:
        combined_frame = combined_frame.union(frame)
    return combined_frame

def write_dynamic_frame_to_s3(dynamic_frame, target_table, output_path):
    """Write the combined DynamicFrame to the target S3 bucket."""
    glueContext.write_dynamic_frame.from_options(
        dynamic_frame, 
        connection_type="s3", 
        connection_options={"path": output_path},
        format="parquet"  # Adjust format if needed
    )
    logger.info(f"Written data to S3 at {output_path}")

# Main execution
try:
    table_names = get_table_names(source_database_name)
    with ThreadPoolExecutor(max_workers=max_concurrent_queries) as executor:
        futures = []
        for suffix, combined_table_name in combined_table_names.items():
            # Filter tables for the current suffix
            relevant_tables = [t for t in table_names if t.endswith(suffix)]
            dynamic_frames = []

            for table_name in relevant_tables:
                dynamic_frame = read_table_to_dynamic_frame(table_name, source_database_name)
                dynamic_frames.append(dynamic_frame)

            if dynamic_frames:
                combined_frame = combine_dynamic_frames(dynamic_frames)
                output_path = f"{output_bucket}/{combined_table_name}/"
                
                semaphore.acquire()  # Limit concurrency
                future = executor.submit(write_dynamic_frame_to_s3, combined_frame, combined_table_name, output_path)
                future.add_done_callback(lambda p: semaphore.release())
                futures.append(future)

        # Wait for all futures to complete
        for future in as_completed(futures):
            future.result()  # Raise any exceptions
except Exception as e:
    logger.error(f"Error occurred: {str(e)}")

job.commit()