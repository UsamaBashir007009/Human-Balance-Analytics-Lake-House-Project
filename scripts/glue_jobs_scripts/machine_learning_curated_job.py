import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1737619255765 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1737619255765")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1737619256329 = glueContext.create_dynamic_frame.from_catalog(database="human-balance-analytics-lakehouse-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1737619256329")

# Script generated for node ML Curated Join
MLCuratedJoin_node1737619328257 = Join.apply(frame1=StepTrainerTrusted_node1737619255765, frame2=AccelerometerTrusted_node1737619256329, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="MLCuratedJoin_node1737619328257")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=MLCuratedJoin_node1737619328257, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737619228553", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1737619482822 = glueContext.getSink(path="s3://human-balance-lakehouse/machine-learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1737619482822")
MachineLearningCurated_node1737619482822.setCatalogInfo(catalogDatabase="human-balance-analytics-lakehouse-db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1737619482822.setFormat("json")
MachineLearningCurated_node1737619482822.writeFrame(MLCuratedJoin_node1737619328257)
job.commit()