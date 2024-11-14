import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_OUTPUT_PATH', 'GLUE_DATABASE', 'GLUE_TABLE_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set parameters
s3_output_path = args['S3_OUTPUT_PATH']  
glue_database = args['GLUE_DATABASE']  
glue_table_name = args['GLUE_TABLE_NAME'] 

# data from old SQL database into a DynamicFrame
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=glue_database, 
    table_name=glue_table_name,
    transformation_ctx="datasource"
)

#other part of the script

# data to S3 in CSV format
glueContext.write_dynamic_frame.from_options(
    frame=datasource,
    connection_type="s3",
    connection_options={"path": s3_output_path},
    format="csv",
    transformation_ctx="datasink"
)

job.commit()
