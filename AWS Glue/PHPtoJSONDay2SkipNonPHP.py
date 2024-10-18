#2024-10-18 Alex Day Transformed PHP serialized data to JSON format, skipping non-PHP serialized data
import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import phpserialize

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the table from Glue Data Catalog
try:
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database="combinedtables", 
        table_name="combined_conf"
    )
    print("Successfully loaded dynamic frame.")
except Exception as e:
    print(f"Error loading dynamic frame: {str(e)}")
    raise  # Rethrow the error to stop the job

# Convert DynamicFrame to DataFrame for easier transformation
df = dynamic_frame.toDF()

# Helper function to convert byte keys and values to string keys and values
def convert_keys_to_str(data):
    try:
        if isinstance(data, dict):
            return {k.decode('utf-8') if isinstance(k, bytes) else k: convert_keys_to_str(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_keys_to_str(i) for i in data]
        elif isinstance(data, bytes):
            return data.decode('utf-8')
        else:
            return data
    except Exception as e:
        print(f"Error converting keys: {str(e)}")
        return {"error": "key conversion failed"}

# Define a UDF to deserialize PHP and convert to JSON, with better error handling
def php_to_json(serialized_php):
    if not serialized_php:
        return ""

    # Check if the value is PHP serialized
    if not (serialized_php.startswith('a:') or serialized_php.startswith('s:') or 
            serialized_php.startswith('i:') or serialized_php.startswith('d:') or 
            serialized_php.startswith('O:') or serialized_php.startswith('b:') or 
            serialized_php.startswith('N;')):
        return serialized_php  # Return the value as is if it's not PHP serialized

    try:
        # Log the input value for debugging
        print(f"Attempting to deserialize: {serialized_php}")
        
        # Deserialize the PHP serialized data
        deserialized_data = phpserialize.loads(serialized_php.encode('utf-8'))
        
        # Convert byte keys and values to string keys and values
        cleaned_data = convert_keys_to_str(deserialized_data)

        # Convert to JSON string
        return json.dumps(cleaned_data)

    except Exception as e:
        # Log the error and input value for deserialization issues
        print(f"Error deserializing value: {serialized_php}. Error: {str(e)}")
        return json.dumps({"error": str(e)})

# Register the UDF
php_to_json_udf = udf(php_to_json, StringType())

# Apply the UDF to transform the "value" column
try:
    # Log the schema and sample data before transformation
    df.printSchema()
    df.show(5)
    
    df_transformed = df.withColumn("value", php_to_json_udf(col("value")))
    print("Successfully transformed the 'value' column.")
except Exception as e:
    print(f"Error transforming 'value' column: {str(e)}")
    raise

# Convert back to DynamicFrame
try:
    dynamic_frame_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dynamic_frame_transformed")
    print("Successfully converted DataFrame to DynamicFrame.")
except Exception as e:
    print(f"Error converting to DynamicFrame: {str(e)}")
    raise

# Write the transformed data to the S3 bucket in JSON format
try:
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_transformed,
        connection_type="s3",
        connection_options={"path": "s3://phptojsontest/"},
        format="json"
    )
    print("Successfully wrote the transformed data to the S3 bucket.")
except Exception as e:
    print(f"Error writing to S3: {str(e)}")
    raise

# Commit the job
job.commit()
print("Job committed successfully.")