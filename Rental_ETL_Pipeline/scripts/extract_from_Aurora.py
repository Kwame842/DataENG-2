import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['extract_from_Aurora'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['extract_from_Aurora'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Relational DB
RelationalDB_node1750167376145 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750167376145"
)

# Script generated for node Relational DB
RelationalDB_node1750168442352 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750168442352"
)

# Script generated for node Relational DB
RelationalDB_node1750168611895 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewing",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750168611895"
)

# Script generated for node Relational DB
RelationalDB_node1750168297026 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750168297026"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750167376145, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750167369157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750167376145.count() >= 1):
   RelationalDB_node1750167376145 = RelationalDB_node1750167376145.coalesce(1)
AmazonS3_node1750168210055 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750167376145, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-etl-pipeline-data/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750168210055")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750168442352, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750167369157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750168442352.count() >= 1):
   RelationalDB_node1750168442352 = RelationalDB_node1750168442352.coalesce(1)
AmazonS3_node1750168549302 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750168442352, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-etl-pipeline-data/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750168549302")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750168611895, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750167369157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750168611895.count() >= 1):
   RelationalDB_node1750168611895 = RelationalDB_node1750168611895.coalesce(1)
AmazonS3_node1750168716947 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750168611895, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-etl-pipeline-data/raw/user_viewing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750168716947")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750168297026, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750167369157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750168297026.count() >= 1):
   RelationalDB_node1750168297026 = RelationalDB_node1750168297026.coalesce(1)
AmazonS3_node1750168358990 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750168297026, connection_type="s3", format="glueparquet", connection_options={"path": "s3://rental-etl-pipeline-data/raw/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750168358990")

job.commit()