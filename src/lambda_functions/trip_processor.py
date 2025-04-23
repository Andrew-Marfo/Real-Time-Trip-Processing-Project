import json
import boto3
import base64

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

def lambda_handler(event, context):
    for record in event['Records']:
        # Decode Kinesis record
        payload = base64.b64decode(record['kinesis']['data'])
        data = json.loads(payload)
        
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
                print(f"Trip start not found for trip_id: {trip_id}")
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
            
            # Trigger downstream processing (e.g., publish to another Kinesis stream for aggregation)
            print(f"Trip {trip_id} completed: {trip_data}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processed records successfully')
    }