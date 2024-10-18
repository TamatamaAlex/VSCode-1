#DOES NOT WORK
import sys
import boto3
import json
import phpserialize  # Ensure this library is available in the Glue environment.
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue and Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue catalog
combined_conf_df = glueContext.create_dynamic_frame.from_catalog(
    database="combinedtables", 
    table_name="combined_conf"
).toDF()

# Define a UDF to convert PHP serialized strings to JSON
def php_to_json(php_string):
    if php_string is None:
        return None  # Handle null values
    try:
        # Deserialize the PHP string to a Python dictionary
        deserialized = phpserialize.loads(php_string.encode('utf-8'))
        # Convert to JSON string
        json_string = json.dumps(deserialized) #print
        return json_string
    except Exception as e:
        # Log the error and return the original string
        print(f"Error deserializing PHP string: {e}")
        return php_string

# Register the function as a UDF
php_to_json_udf = udf(php_to_json, StringType())

# Apply the UDF to transform the 'value' column
transformed_df = combined_conf_df.withColumn("value", php_to_json_udf(col("value")))

# Convert back to DynamicFrame
transformed_dynamic_df = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_df")

# Write the transformed data to the specified S3 location in JSON format
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_df,
    connection_type="s3",
    connection_options={
        "path": "s3://phptojsontest/"
    },
    format="json"
)

# Commit the job
job.commit()
