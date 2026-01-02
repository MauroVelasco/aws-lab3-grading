import json
import urllib.parse
import boto3
import os

# Initialize the SQS client outside the handler for better performance
sqs = boto3.client('sqs')
QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

def lambda_handler(event, context):
    try:
        # 1. Iterate through the records (S3 events can sometimes be batched)
        for record in event['Records']:
            # 2. Extract Bucket Name
            bucket_name = record['s3']['bucket']['name']
            
            # 3. Extract and Decode Object Key (important for spaces/special chars)
            raw_key = record['s3']['object']['key']
            object_key = urllib.parse.unquote_plus(raw_key, encoding='utf-8')
            
            # 4. Extract ETag
            object_etag = record['s3']['object'].get('eTag', 'N/A')
            
            # 5. Filter for PNG files only (optional but recommended)
            if not object_key.lower().endswith('.png'):
                print(f"Skipping non-png file: {object_key}")
                continue
                
            # 6. Construct the message
            message_body = {
                "bucket": bucket_name,
                "key": object_key,
                "etag": object_etag,
                "event_time": record['eventTime']
            }
            
            # 7. Send to SQS
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            
            print(f"Successfully sent message for {object_key}. MessageID: {response['MessageId']}")
            
        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e