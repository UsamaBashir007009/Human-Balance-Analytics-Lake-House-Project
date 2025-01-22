import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1737571656007 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1737571656007")

# Script generated for node Custumer Trusted
CustumerTrusted_node1737571654609 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="customer_trusted", transformation_ctx="CustumerTrusted_node1737571654609")

# Script generated for node Accelerometer Trusted
SqlQuery0 = '''
SELECT user, timestamp, x, y, z
FROM accelerometer_landing JOIN customer_trusted
ON accelerometer_landing.user = customer_trusted.email
'''
AccelerometerTrusted_node1737574164055 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":AccelerometerLanding_node1737571656007, "customer_trusted":CustumerTrusted_node1737571654609}, transformation_ctx = "AccelerometerTrusted_node1737574164055")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AccelerometerTrusted_node1737574164055, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737573896135", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737574392199 = glueContext.getSink(path="s3://human-balance-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737574392199")
AmazonS3_node1737574392199.setCatalogInfo(catalogDatabase="human-balance-analytics-lakehouse-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1737574392199.setFormat("json")
AmazonS3_node1737574392199.writeFrame(AccelerometerTrusted_node1737574164055)
job.commit()