import boto3
import logging
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# AWS settings
region = "ap-northeast-1"
source_database_name = "glueDBName"
output_bucket = "s3://YourBucketName/"
combined_table_names = {
    "suffix_name": "output table name"
}
max_partitions = 200  # Optimize shuffle partitions

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('optimized_glue_job', {})

# Set logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Optimize Spark configurations
spark.conf.set("spark.sql.shuffle.partitions", max_partitions)  # Reduce shuffle overhead
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable broadcast joins

def get_table_names(database_name):
    """Fetch table names from Glue Data Catalog."""
    glue_client = boto3.client('glue', region_name=region)
    paginator = glue_client.get_paginator('get_tables')
    tables = [
        table['Name']
        for page in paginator.paginate(DatabaseName=database_name)
        for table in page['TableList']
    ]
    return tables

def get_table_s3_path(table_name, database_name):
    """Retrieve the S3 path for a given table from Glue Data Catalog."""
    glue_client = boto3.client('glue', region_name=region)
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    return response['Table']['StorageDescriptor']['Location']

def read_table_to_df_with_grouping(s3_path, table_name):
    """Read a table into a Spark DataFrame using file grouping and a dynamically fetched S3 path."""
    logger.info(f"Reading data from S3 path: {s3_path}")
    
    # Use file grouping to optimize the read process
    dynamic_frame = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={
            'paths': [s3_path],
            'recurse': True,
            'groupFiles': 'inPartition',
            'groupSize': '1048576'  # Group files to process in batches of ~1 MB
        },
        format="parquet"  # Assuming the files are in Parquet format
    )
    df = dynamic_frame.toDF().withColumn("source_table", F.lit(table_name))
    return df

def write_df_to_s3(df, output_path, partitions=10):
    """Write the DataFrame to S3 in optimized partitions."""
    df.coalesce(partitions).write \
        .mode("overwrite") \
        .format("parquet") \
        .save(output_path)
    logger.info(f"Data written to {output_path}")

try:
    # Fetch all table names from the source database
    table_names = get_table_names(source_database_name)

    for suffix, combined_table_name in combined_table_names.items():
        # Filter tables by suffix
        matching_tables = [t for t in table_names if t.endswith(suffix)]
        if not matching_tables:
            logger.warning(f"No matching tables found for suffix: {suffix}")
            continue

        logger.info(f"Processing {len(matching_tables)} tables for suffix: {suffix}")
        
        combined_df = None
        # Read and combine all matching tables into one DataFrame
        for table_name in matching_tables:
            s3_path = get_table_s3_path(table_name, source_database_name)
            df = read_table_to_df_with_grouping(s3_path, table_name)

            if combined_df is None:
                combined_df = df
            else:
                # Use unionByName with allowMissingColumns=True for schema alignment
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        if combined_df is not None:
            # Optimize file writes to S3
            output_path = f"{output_bucket}/{combined_table_name}/"
            write_df_to_s3(combined_df, output_path, partitions=10)
        else:
            logger.warning(f"No data to combine for suffix: {suffix}")

except Exception as e:
    logger.error(f"Error occurred: {str(e)}")

job.commit()