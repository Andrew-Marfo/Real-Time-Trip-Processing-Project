import pandas as pd
import boto3
import json
import base64
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
region = os.getenv('AWS_REGION')

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name=region)  # Replace with your AWS region

# File paths for your local data
trip_start_path = "./../data/trip_start.csv"
trip_end_path = "./../data/trip_end.csv"

# Read the first 10 records from each CSV file
trip_start_df = pd.read_csv(trip_start_path).head(10)
trip_end_df = pd.read_csv(trip_end_path).head(10)

# Function to send data to Kinesis
def send_to_kinesis(stream_name, records):
    for _, row in records.iterrows():
        # Convert the row to a dictionary and then to JSON
        record = row.to_dict()
        record_json = json.dumps(record)
        
        # Encode the JSON string as bytes and then base64 encode for Kinesis
        record_bytes = record_json.encode('utf-8')
        record_b64 = base64.b64encode(record_bytes).decode('utf-8')
        
        # Send to Kinesis
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=record_b64,
            PartitionKey=record['trip_id']
        )
        print(f"Sent record to {stream_name}: {record['trip_id']} - ShardId: {response['ShardId']}")

# Send trip start records
print("Sending trip start records...")
send_to_kinesis('TripStartStream', trip_start_df)

# Send trip end records
# print("Sending trip end records...")
# send_to_kinesis('TripEndStream', trip_end_df)

print("Finished sending records to Kinesis streams.")