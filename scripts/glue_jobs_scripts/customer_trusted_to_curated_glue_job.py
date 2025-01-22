import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Customer Trusted
CustomerTrusted_node1737574792782 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1737574792782")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1737574813466 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1737574813466")

# Script generated for node Curated Customers
SqlQuery0 = '''
SELECT
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM customer_trusted JOIN accelerometer_trusted
ON customer_trusted.email = accelerometer_trusted.user

'''
CuratedCustomers_node1737574839484 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1737574792782, "accelerometer_trusted":AccelerometerTrusted_node1737574813466}, transformation_ctx = "CuratedCustomers_node1737574839484")

# Script generated for node Drop Duplicate Customer
DropDuplicateCustomer_node1737576011287 =  DynamicFrame.fromDF(CuratedCustomers_node1737574839484.toDF().dropDuplicates(), glueContext, "DropDuplicateCustomer_node1737576011287")

# Script generated for node Store Data in Curated Customers
EvaluateDataQuality().process_rows(frame=DropDuplicateCustomer_node1737576011287, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737573896135", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StoreDatainCuratedCustomers_node1737576126226 = glueContext.getSink(path="s3://human-balance-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StoreDatainCuratedCustomers_node1737576126226")
StoreDatainCuratedCustomers_node1737576126226.setCatalogInfo(catalogDatabase="human-balance-analytics-lakehouse-db",catalogTableName="customers_curated")
StoreDatainCuratedCustomers_node1737576126226.setFormat("json")
StoreDatainCuratedCustomers_node1737576126226.writeFrame(DropDuplicateCustomer_node1737576011287)
job.commit()