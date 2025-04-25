import json
import boto3
import logging
import base64
from decimal import Decimal

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

# Function to convert float objects to decimal
def convert_floats_to_decimals(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_floats_to_decimals(v) for v in obj]
    return obj

def lambda_handler(event, context):
    logger.info("Lambda function started")
    for record in event['Records']:
        try:
            # Decode Kinesis record (base64-encoded string to bytes, then to JSON)
            payload_b64 = record['kinesis']['data']  # Base64-encoded string
            payload_bytes = base64.b64decode(payload_b64)  # Decode to bytes
            payload = payload_bytes.decode('utf-8')  # Decode bytes to UTF-8 string
            data = json.loads(payload)  # Parse JSON string to dictionary
            
            # Convert all float values to Decimal
            data = convert_floats_to_decimals(data)
            
            trip_id = data['trip_id']
            stream_name = record['eventSourceARN'].split('/')[-1]  # Extract stream name
            
            if stream_name == 'TripStartStream':
                # Process trip start event
                trip_data = {
                    'trip_id': trip_id,
                    'pickup_location_id': data['pickup_location_id'],
                    'dropoff_location_id': data['dropoff_location_id'],
                    'vendor_id': data['vendor_id'],
                    'pickup_datetime': data['pickup_datetime'],
                    'estimated_dropoff_datetime': data['estimated_dropoff_datetime'],
                    'estimated_fare_amount': data['estimated_fare_amount'],
                    'status': 'Started'
                }
                # Store in DynamoDB
                table.put_item(Item=trip_data)
            
            elif stream_name == 'TripEndStream':
                # Process trip end event
                # Check if trip start exists in DynamoDB
                response = table.get_item(Key={'trip_id': trip_id})
                if 'Item' not in response:
                    logger.warning(f"Trip start not found for trip_id: {trip_id}")
                    continue
                
                # Update the existing record with trip end data
                trip_data = response['Item']
                trip_data.update({
                    'dropoff_datetime': data['dropoff_datetime'],
                    'rate_code': data['rate_code'],
                    'passenger_count': data['passenger_count'],
                    'trip_distance': data['trip_distance'],
                    'fare_amount': data['fare_amount'],
                    'tip_amount': data['tip_amount'],
                    'payment_type': data['payment_type'],
                    'trip_type': data['trip_type'],
                    'status': 'Completed'
                })
                # Store updated record in DynamoDB
                table.put_item(Item=trip_data)
                
                # Log the completed trip
                logger.info(f"Trip {trip_id} completed: {trip_data}")
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processed records successfully')
    }