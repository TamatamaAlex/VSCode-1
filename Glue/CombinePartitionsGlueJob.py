#This script combines partitions from multiple tables in the Glue catalog and writes the combined data to S3 in Parquet format.
import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get job arguments (e.g., Job Name)
args = getResolvedOptions(sys.argv, ['TransformTestOrders'])
job_name = args['TransformTestOrders']

# Define the database and partition names we are interested in
database_name = "auroratos3orderdatatest"
partitioned_tables = ["ota.order", "learning.order", "azama.order"]

# Load all relevant partitions from Glue Catalog
dynamic_frames = []
for partition in partitioned_tables:
    table_name, partition_key = partition.split(".")
    
    # Create a DynamicFrame for each partition
    df = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx=partition_key
    )
    
    # Add the loaded frame to our list
    dynamic_frames.append(df)

# Combine all DynamicFrames using union
combined_dynamic_frame = dynamic_frames[0]
for df in dynamic_frames[1:]:
    combined_dynamic_frame = combined_dynamic_frame.union(df)

# Write the combined data to S3 in Parquet format
output_path = "s3://s3 bucket name here/"
glueContext.write_dynamic_frame.from_options(
    frame=combined_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

print("Glue job completed successfully!")
