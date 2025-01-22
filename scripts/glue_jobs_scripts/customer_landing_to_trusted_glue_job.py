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

# Script generated for node CustomerLandingS3
CustomerLandingS3_node1737464326282 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://human-balance-lakehouse/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingS3_node1737464326282")

# Script generated for node CustomerPrivacy
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null;
'''
CustomerPrivacy_node1737464600808 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLandingS3_node1737464326282}, transformation_ctx = "CustomerPrivacy_node1737464600808")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=CustomerPrivacy_node1737464600808, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737463675428", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737464695775 = glueContext.getSink(path="s3://human-balance-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737464695775")
AmazonS3_node1737464695775.setCatalogInfo(catalogDatabase="human-balance-analytics-lakehouse-db",catalogTableName="customer_trusted")
AmazonS3_node1737464695775.setFormat("json")
AmazonS3_node1737464695775.writeFrame(CustomerPrivacy_node1737464600808)
job.commit()