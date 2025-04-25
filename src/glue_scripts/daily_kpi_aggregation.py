import sys
import boto3
import logging
from datetime import datetime, date
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, count, avg, max, min, to_date

# Configure logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DYNAMODB_TABLE', 'OUTPUT_S3_PATH'])
dynamodb_table = args['DYNAMODB_TABLE']
output_s3_path = args['OUTPUT_S3_PATH']

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

def fetch_data_from_dynamodb():
    """Fetch completed trip data from DynamoDB"""
    try:
        logger.info(f"Fetching data from DynamoDB table: {dynamodb_table}")
        table = dynamodb.Table(dynamodb_table)
        
        # Scan DynamoDB for completed trips (status = 'Completed')
        response = table.scan(
            FilterExpression='#status = :status_val',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status_val': 'Completed'}
        )
        
        items = response['Items']
        
        # Handle pagination if results are large
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey'],
                FilterExpression='#status = :status_val',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={':status_val': 'Completed'}
            )
            items.extend(response['Items'])
        
        logger.info(f"Fetched {len(items)} completed trips from DynamoDB")
        return items
        
    except Exception as e:
        logger.error(f"Error fetching data from DynamoDB: {str(e)}")
        raise

def transform_data(items):
    """Transform DynamoDB items into Spark DataFrame and compute KPIs"""
    try:
        logger.info("Starting data transformation")
        
        # Convert DynamoDB items to Spark DataFrame
        df = spark.createDataFrame(items)
        
        # Convert Decimal types to float for easier processing
        decimal_cols = ['fare_amount', 'estimated_fare_amount', 'tip_amount']
        for col_name in decimal_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast('float'))
        
        # Extract date from pickup_datetime
        df = df.withColumn('trip_date', to_date(col('pickup_datetime')))
        
        logger.info("Data successfully loaded into Spark DataFrame")
        return df
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def compute_kpis(df):
    """Compute daily KPIs from trip data"""
    try:
        logger.info("Computing daily KPIs")
        
        # Group by date and compute metrics
        kpis = df.groupBy('trip_date').agg(
            sum('fare_amount').alias('total_fare'),
            count('trip_id').alias('count_trips'),
            avg('fare_amount').alias('average_fare'),
            max('fare_amount').alias('max_fare'),
            min('fare_amount').alias('min_fare')
        ).orderBy('trip_date')
        
        logger.info("Successfully computed KPIs")
        return kpis
        
    except Exception as e:
        logger.error(f"Error computing KPIs: {str(e)}")
        raise

def write_to_s3(kpis_df):
    """Write KPIs to S3 in JSON format"""
    try:
        # Get current timestamp for file naming
        date = datetime.now().strftime("%Y_%m_%d")
        output_path = f"{output_s3_path}/{date}.json"
        
        logger.info(f"Writing KPIs to S3: {output_path}")
        
        # Write as single JSON file (coalesce to 1 partition)
        kpis_df.coalesce(1).write.mode('overwrite').json(output_path)
        
        logger.info("Successfully wrote KPIs to S3")
        
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise

def main():
    try:
        logger.info("Starting Glue job execution")
        
        # Initialize job
        job.init(args['JOB_NAME'], args)
        
        # ETL Pipeline
        items = fetch_data_from_dynamodb()
        df = transform_data(items)
        kpis_df = compute_kpis(df)
        write_to_s3(kpis_df)
        
        logger.info("Glue job completed successfully")
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        job.commit()
        raise

if __name__ == "__main__":
    main()