import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['load_to_Redshift_raw'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['load_to_Redshift_raw'], args)

# Script generated for node Amazon S3
AmazonS3_node1750169678926 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-etl-pipeline-data/raw/apartment_attributes/run-1750169196651-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750169678926")

# Script generated for node Amazon S3
AmazonS3_node1750170034048 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-etl-pipeline-data/raw/apartments/run-1750169259841-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750170034048")

# Script generated for node Amazon S3
AmazonS3_node1750170445371 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-etl-pipeline-data/raw/user_viewing/run-1750169228676-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750170445371")

# Script generated for node Amazon S3
AmazonS3_node1750170327154 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://rental-etl-pipeline-data/raw/bookings/run-1750169160079-part-block-0-r-00000-snappy.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1750170327154")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750169760381 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750169678926, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DOUBLE PRECISION, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION); TRUNCATE TABLE raw_layer.apartment_attributes;"}, transformation_ctx="AmazonRedshift_node1750169760381")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750170101980 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750170034048, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DOUBLE PRECISION, currency VARCHAR, listing_created_on VARCHAR, is_active BOOLEAN, last_modified_timestamp VARCHAR); TRUNCATE TABLE raw_layer.apartments;"}, transformation_ctx="AmazonRedshift_node1750170101980")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750170495596 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750170445371, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at VARCHAR, is_wishlisted BOOLEAN, call_to_action VARCHAR); TRUNCATE TABLE raw_layer.user_viewing;"}, transformation_ctx="AmazonRedshift_node1750170495596")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750170386474 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750170327154, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price DOUBLE PRECISION, currency VARCHAR, booking_status VARCHAR); TRUNCATE TABLE raw_layer.bookings;"}, transformation_ctx="AmazonRedshift_node1750170386474")

job.commit()