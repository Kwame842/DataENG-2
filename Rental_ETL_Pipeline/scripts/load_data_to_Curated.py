import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['load_data_to_Curated'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['load_data_to_Curated'], args)

# Script generated for node Amazon Redshift
AmazonRedshift_node1750171369307 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift_connection"}, transformation_ctx="AmazonRedshift_node1750171369307")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750172065212 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift_connection"}, transformation_ctx="AmazonRedshift_node1750172065212")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750172937029 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift_connection"}, transformation_ctx="AmazonRedshift_node1750172937029")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750172241516 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift_connection"}, transformation_ctx="AmazonRedshift_node1750172241516")

# Script generated for node Drop Duplicates
DropDuplicates_node1750171459058 =  DynamicFrame.fromDF(AmazonRedshift_node1750171369307.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750171459058")

# Script generated for node Drop Duplicates
DropDuplicates_node1750172141900 =  DynamicFrame.fromDF(AmazonRedshift_node1750172065212.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750172141900")

# Script generated for node Drop Duplicates
DropDuplicates_node1750173006187 =  DynamicFrame.fromDF(AmazonRedshift_node1750172937029.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750173006187")

# Script generated for node Drop Duplicates
DropDuplicates_node1750172310326 =  DynamicFrame.fromDF(AmazonRedshift_node1750172241516.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750172310326")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750171710557 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1750171459058, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id VARCHAR, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee VARCHAR, has_photo BOOLEAN, pets_allowed BOOLEAN, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, price_type VARCHAR, price_display VARCHAR, id_int INTEGER, fee_double DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750171710557")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750172210643 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1750172141900, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id VARCHAR, title VARCHAR, source VARCHAR, price VARCHAR, currency VARCHAR, listing_created_on VARCHAR, is_active BOOLEAN, last_modified_timestamp VARCHAR, listing_created_on_string VARCHAR, id_int INTEGER, last_modified_timestamp_string VARCHAR, price_double DOUBLE PRECISION); TRUNCATE TABLE curated.apartments;"}, transformation_ctx="AmazonRedshift_node1750172210643")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750173027204 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1750173006187, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewing", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing (user_id VARCHAR, apartment_id VARCHAR, viewed_at VARCHAR, is_wishlisted BOOLEAN, call_to_action VARCHAR, apartment_id_int INTEGER, viewed_at_string VARCHAR, user_id_int INTEGER); TRUNCATE TABLE curated.user_viewing;"}, transformation_ctx="AmazonRedshift_node1750173027204")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750172416397 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1750172310326, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-714377355835-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift_connection", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id VARCHAR, user_id VARCHAR, apartment_id VARCHAR, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price VARCHAR, currency VARCHAR, booking_status VARCHAR, payment_status VARCHAR, num_guests VARCHAR, booking_date_string VARCHAR, booking_id_int INTEGER, checkin_date_string VARCHAR, checkout_date_string VARCHAR, apartment_id_int INTEGER, user_id_int INTEGER, total_price_double DOUBLE PRECISION); TRUNCATE TABLE curated.bookings;"}, transformation_ctx="AmazonRedshift_node1750172416397")

job.commit()