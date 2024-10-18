#DOES NOT WORK
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import phpserialize

# Initialize Glue Context and Spark Session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Data from Glue Table
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="combinedtables", 
    table_name="combined_conf"
)

# Print schema of the data source
datasource.printSchema()

# Print sample data before transformation
print("Sample data before transformation:")
datasource.show(5)

# Transform Data: Convert PHP to JSON
def php_to_json(record):
    php_data = record["value"]
    json_data = json.dumps(phpserialize.loads(php_data.encode('utf-8')))
    record["value"] = json_data
    return record

mapped_dyF = Map.apply(frame=datasource, f=php_to_json)

# Print sample data after transformation
print("Sample data after transformation:")
mapped_dyF.show(5)

# Write Data to S3 Bucket
sink = glueContext.write_dynamic_frame.from_options(
    frame=mapped_dyF,
    connection_type="s3",
    connection_options={"path": "s3://phptojsontest/"},
    format="json"
)

job.commit()