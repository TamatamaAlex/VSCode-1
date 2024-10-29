#This joins the tables from the Glue catalog and writes the combined data to S3 in Parquet format.
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['TransformTestOrders'])  # Use your Glue job name here
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['TransformTestOrders'], args)

# Load data from Glue catalog
ota_df = glueContext.create_dynamic_frame.from_catalog(database="auroratos3orderdatatest", table_name="ota")
learning_df = glueContext.create_dynamic_frame.from_catalog(database="auroratos3orderdatatest", table_name="learning")
azama_df = glueContext.create_dynamic_frame.from_catalog(database="auroratos3orderdatatest", table_name="azama")

# Convert to DataFrame for SQL operations
ota_spark_df = ota_df.toDF()
learning_spark_df = learning_df.toDF()
azama_spark_df = azama_df.toDF()

# Perform joins on common keys (adjust the join conditions as necessary)
combined_df = ota_spark_df \
    .join(learning_spark_df, on=["order", "order_totals", "order_products"], how="outer") \
    .join(azama_spark_df, on=["order", "order_totals", "order_products"], how="outer")

# Convert back to DynamicFrame
combined_dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "combined_dynamic_frame")

# Write the combined data to S3 in Parquet format
combined_dynamic_frame.write.mode("overwrite").parquet("s3://gluejobtestbucketparquet")

job.commit()
