import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, sum, count, avg, max, min
from datetime import datetime, timedelta

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from DynamoDB
dynamodb_table = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="TripData",
    transformation_ctx="dynamodb_table"
)

# Convert to DataFrame
df = dynamodb_table.toDF()

# Filter for completed trips
completed_trips = df.filter(col("status") == "Completed")

# Extract the date from dropoff_datetime (format: DD/MM/YYYY HH:MM)
completed_trips = completed_trips.withColumn(
    "dropoff_date",
    to_date(col("dropoff_datetime"), "dd/MM/yyyy HH:mm")
)

# Compute KPIs for the previous day
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
kpi_df = completed_trips.filter(col("dropoff_date") == yesterday).groupBy("dropoff_date").agg(
    sum("fare_amount").alias("total_fare"),
    count("*").alias("count_trips"),
    avg("fare_amount").alias("average_fare"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare")
)

# Convert to JSON format
kpi_json = kpi_df.toJSON().collect()

# Write to S3
if kpi_json:
    kpi_data = [json.loads(record) for record in kpi_json][0]
    kpi_data.pop("dropoff_date")  # Remove the date from the JSON content
    glueContext.write_dynamic_frame.from_options(
        frame=glueContext.create_dynamic_frame.from_catalog(
            database="default",
            table_name="TripData"
        ).filter(col("status") == "Completed").limit(1),  # Dummy frame to use write_dynamic_frame
        connection_type="s3",
        connection_options={
            "path": f"s3://nsp-bolt-ride-kpis/kpis/{yesterday}/",
            "partitionKeys": []
        },
        format="json",
        transformation_ctx="write_to_s3"
    )
    # Overwrite with the actual KPI JSON
    spark.createDataFrame([kpi_data], "struct<total_fare:double,count_trips:long,average_fare:double,max_fare:double,min_fare:double>").write.mode("overwrite").json(f"s3://nsp-bolt-ride-kpis/kpis/{yesterday}/metrics.json")

job.commit()