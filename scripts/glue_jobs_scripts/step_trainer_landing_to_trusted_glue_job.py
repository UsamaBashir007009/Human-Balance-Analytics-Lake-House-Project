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

# Script generated for node Customers Curated Catalog
CustomersCuratedCatalog_node1737576754630 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="customers_curated", transformation_ctx="CustomersCuratedCatalog_node1737576754630")

# Script generated for node Step Trainer Landing Catalog
StepTrainerLandingCatalog_node1737576717929 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingCatalog_node1737576717929")

# Script generated for node Step Trainer Trusted Query
SqlQuery0 = '''
SELECT
    step_trainer_landing.sensorreadingtime,
    step_trainer_landing.serialnumber,
    step_trainer_landing.distancefromobject
FROM step_trainer_landing JOIN customer_curated
ON step_trainer_landing.serialnumber = customer_curated.serialnumber
'''
StepTrainerTrustedQuery_node1737576789298 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":StepTrainerLandingCatalog_node1737576717929, "customer_curated":CustomersCuratedCatalog_node1737576754630}, transformation_ctx = "StepTrainerTrustedQuery_node1737576789298")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=StepTrainerTrustedQuery_node1737576789298, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737576679752", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1737577457049 = glueContext.getSink(path="s3://human-balance-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1737577457049")
StepTrainerTrusted_node1737577457049.setCatalogInfo(catalogDatabase="human-balance-analytics-lakehouse-db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1737577457049.setFormat("json")
StepTrainerTrusted_node1737577457049.writeFrame(StepTrainerTrustedQuery_node1737576789298)
job.commit()