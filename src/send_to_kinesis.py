import pandas as pd
import boto3
import json
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()
region = os.getenv('AWS_REGION')

# Initialize Kinesis client
try:
    kinesis_client = boto3.client('kinesis', region_name=region)
except Exception as e:
    print(f"Error initializing Kinesis client: {e}")
    exit(1)

# File paths for trip start and end data
trip_start_path = "data/trip_start.csv" 
trip_end_path = "data/trip_end.csv"

# Read all records from each CSV file
try:
    print("Reading trip start data...")
    trip_start_df = pd.read_csv(trip_start_path)
    print(f"Successfully read {len(trip_start_df)} trip start records.")
except Exception as e:
    print(f"Error reading trip_start.csv: {e}")
    exit(1)

try:
    print("Reading trip end data...")
    trip_end_df = pd.read_csv(trip_end_path)
    print(f"Successfully read {len(trip_end_df)} trip end records.")
except Exception as e:
    print(f"Error reading trip_end.csv: {e}")
    exit(1)

# Function to send data to Kinesis
def send_to_kinesis(stream_name, records):
    sent_count = 0
    for _, row in records.iterrows():
        try:
            # Convert the row to a dictionary and then to JSON
            record = row.to_dict()
            record_json = json.dumps(record)
            
            # Send the JSON string as raw bytes
            record_bytes = record_json.encode('utf-8')
            
            # Send to Kinesis
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=record_bytes,
                PartitionKey=str(record['trip_id'])  # Ensure trip_id is string
            )
            sent_count += 1
            
            # Print progress every 100 records
            if sent_count % 100 == 0:
                print(f"Sent {sent_count} records to {stream_name}")
                
        except Exception as e:
            print(f"Error sending record to {stream_name} for trip_id {record.get('trip_id', 'UNKNOWN')}: {e}")
    
    print(f"Finished sending {sent_count} records to {stream_name}")

# Send trip start records
print("\nSending trip start records...")
send_to_kinesis('TripStartStream', trip_start_df)

# Add a 5-minute delay
print("\nWaiting for 5 minutes before sending trip end records...")
time.sleep(300)

# Send trip end records
print("\nSending trip end records...")
send_to_kinesis('TripEndStream', trip_end_df)

print("\nFinished sending all records to Kinesis streams.")