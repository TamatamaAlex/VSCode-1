#THIS IS BROKEN, DO NOT USE
import sys
import json
import phpserialize  # Ensure this package is available in Glue
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Glue database and table names
database_name = "combinedtables"
table_name = "combined_conf"

# Function to convert PHP-serialized data to JSON
def php_to_json(php_serialized_str):
    try:
        deserialized_data = phpserialize.loads(php_serialized_str.encode('utf-8'))
        return json.dumps(deserialized_data)  # Convert deserialized data to JSON
    except Exception as e:
        print(f"Error deserializing: {e}")
        return None

# Register the UDF
php_to_json_udf = udf(php_to_json, StringType())

# Load the table data into a DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name
)

# Convert DynamicFrame to DataFrame
df = dynamic_frame.toDF()

# Apply the PHP-to-JSON transformation on the 'value' column using UDF
transformed_df = df.withColumn("value_json", php_to_json_udf(col("value")))

# Select only the columns you want to keep
result_df = transformed_df.select("value", "value_json")

# Convert the transformed DataFrame back to a DynamicFrame
result_dynamic_frame = DynamicFrame.fromDF(result_df, glueContext, "result_dynamic_frame")

# Write the transformed data back to S3 in JSON format
glueContext.write_dynamic_frame.from_options(
    frame=result_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://phptojsontest/"},
    format="json"
)

# Commit the Glue job
job.commit()