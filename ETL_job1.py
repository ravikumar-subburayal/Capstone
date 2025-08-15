import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
customers_df = spark.sql("SELECT * from capstonedb.custsource")
customers_df = customers_df.dropDuplicates(["customer_id"])
customers_df = customers_df.filter(customers_df.email.isNotNull() & customers_df.zip_code.isNotNull())
customers_df.write.mode("overwrite").parquet("s3://capstoneproject/target/temp/")
job.commit()
