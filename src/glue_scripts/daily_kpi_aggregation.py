import sys
import boto3
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum, count, avg, max, min, to_date
import json

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

def get_previous_day():
    """Get the previous day's date in YYYY-MM-DD format"""
    previous_day = datetime.now() - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")

def fetch_data_for_date(target_date):
    """Fetch completed trip data from DynamoDB for a specific date"""
    try:
        logger.info(f"Fetching data for date: {target_date}")
        table = dynamodb.Table(dynamodb_table)
        
        # Query DynamoDB for items matching the date and status
        response = table.query(
            KeyConditionExpression='#date = :date_val',
            FilterExpression='#status = :status_val',
            ExpressionAttributeNames={
                '#date': 'date',
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':date_val': target_date,
                ':status_val': 'Completed'
            }
        )
        
        items = response['Items']
        
        # Handle pagination if results are large
        while 'LastEvaluatedKey' in response:
            response = table.query(
                ExclusiveStartKey=response['LastEvaluatedKey'],
                KeyConditionExpression='#date = :date_val',
                FilterExpression='#status = :status_val',
                ExpressionAttributeNames={
                    '#date': 'date',
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':date_val': target_date,
                    ':status_val': 'Completed'
                }
            )
            items.extend(response['Items'])
        
        logger.info(f"Fetched {len(items)} completed trips for {target_date}")
        return items
        
    except Exception as e:
        logger.error(f"Error fetching data from DynamoDB: {str(e)}")
        raise

def transform_data(items):
    """Transform DynamoDB items into Spark DataFrame"""
    try:
        logger.info("Starting data transformation")
        
        # Convert DynamoDB items to Spark DataFrame
        df = spark.createDataFrame(items)
        
        # Convert Decimal types to float for easier processing
        decimal_cols = ['fare_amount', 'estimated_fare_amount', 'tip_amount']
        for col_name in decimal_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast('float'))
        
        logger.info("Data successfully loaded into Spark DataFrame")
        return df
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def compute_kpis(df, target_date):
    """Compute KPIs from trip data and format output"""
    try:
        logger.info("Computing KPIs")
        
        # Compute metrics (since we're already filtering by date, no need to group)
        metrics = {
            "trip_date": target_date,
            "total_fare": float(df.agg(sum('fare_amount')).collect()[0][0]),
            "count_trips": df.count(),
            "average_fare": float(df.agg(avg('fare_amount')).collect()[0][0]),
            "max_fare": float(df.agg(max('fare_amount')).collect()[0][0]),
            "min_fare": float(df.agg(min('fare_amount')).collect()[0][0])
        }
        
        # Create final output structure
        output = {
            "date": target_date,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info("Successfully computed KPIs")
        return output
        
    except Exception as e:
        logger.error(f"Error computing KPIs: {str(e)}")
        raise

def write_to_s3(output_data, target_date):
    """Write KPIs to S3 in JSON format"""
    try:
        year_month = target_date[:7]
        output_path = f"{output_s3_path}{year_month}/{target_date}.json"
        json_data = json.dumps(output_data, indent=2)
        
        logger.info(f"Writing KPIs to S3: {output_path}")
        
        # Extract bucket and key from S3 path
        if output_s3_path.startswith('s3://'):
            parts = output_s3_path[5:].split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ''
            key = f"{prefix}{year_month}/{target_date}.json" if prefix else f"{year_month}/{target_date}.json"
        else:
            raise ValueError("Invalid S3 path format. Must start with 's3://'")
        
        # Write directly as JSON file
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_data
        )
        
        logger.info("Successfully wrote KPIs to S3")
        
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise

def main():
    try:
        logger.info("Starting Glue job execution")
        
        # Initialize job
        job.init(args['JOB_NAME'], args)
        
        # Get target date (using hardcoded test date for now)
        target_date = "2024-05-25"  # For testing
        # In production, use: target_date = get_previous_day()
        
        # ETL Pipeline
        items = fetch_data_for_date(target_date)
        if not items:
            logger.info(f"No completed trips found for {target_date}")
            job.commit()
            return
            
        df = transform_data(items)
        output_data = compute_kpis(df, target_date)
        write_to_s3(output_data, target_date)
        
        logger.info("Glue job completed successfully")
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        job.commit()
        raise

if __name__ == "__main__":
    main()