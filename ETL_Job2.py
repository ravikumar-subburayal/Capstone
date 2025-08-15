import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import count, sum, date_format, to_date
 
## @params: [JOB_NAME]

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
src_customer_df = spark.read.parquet("s3:/capstone-ravikumar/output/temp/cleansed_customer_data/")
src_transaction_df = spark.sql("select * from capstone_project_db.transactions_dataset_csv")
geo_dyn_df = glueContext.create_dynamic_frame.from_catalog(
    database="capstone_project_db",    table_name="geolocation_dataset_json")

src_geolocation_df = geo_dyn_df.toDF()
join_cust_geo_df = src_customer_df.join(src_geolocation_df, src_customer_df.zip_code == src_geolocation_df.zip_code, "left")
sel_cust_geo_df = join_cust_geo_df.select(src_customer_df.customer_id, src_customer_df.name, src_customer_df.email, src_customer_df.created_at, src_geolocation_df.city, src_geolocation_df.state, src_customer_df.zip_code)
agg_trans_df = src_transaction_df.groupBy("customer_id").agg(count("customer_id").alias("transaction_count"),
    sum("transaction_amount").alias("total_transaction_amount")
)

join_cust_trans_df = sel_cust_geo_df.join(agg_trans_df, sel_cust_geo_df.customer_id == agg_trans_df.customer_id, "left")
sel_cust_trans_df = join_cust_trans_df.select(sel_cust_geo_df.customer_id, sel_cust_geo_df.name, sel_cust_geo_df.email, sel_cust_geo_df.created_at, sel_cust_geo_df.city, sel_cust_geo_df.state, sel_cust_geo_df.zip_code, agg_trans_df.transaction_count, agg_trans_df.total_transaction_amount)
cust_year_df = sel_cust_trans_df.withColumn("created_year", date_format(to_date("created_at", "yyyy-MM-dd"), "yyyy"))
cust_year_df.printSchema()
cust_year_df.show()
cust_year_df.write.mode("overwrite").partitionBy("state", "created_year").parquet("s3:/capstone-ravikumar/output/Modified_customer_data/")

job.commit()


