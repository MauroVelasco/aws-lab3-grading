import json
import boto3
import io
import os
from PIL import Image
from PIL.ExifTags import TAGS

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print(f"app processing event: {event}")

    for record in event['Records']:
        # 1. Parse SQS message body
        body = json.loads(record['body'])
        bucket = body['bucket']
        key = body['key']
        etag = body['etag']
        
        print(f"Processing {key} from {bucket}")

        # 2. Get the object from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        image_data = response['Body'].read()

        # 3. Open image and extract metadata
        with Image.open(io.BytesIO(image_data)) as img:
            metadata = {
                "filename": os.path.basename(key),
                "original_etag": etag,
                "format": img.format,
                "size": img.size, # (width, height)
                "mode": img.mode,
                "exif": {}
            }
            
            # Extract EXIF data (if available)
            exif_data = img.getexif()
            if exif_data:
                for tag_id, value in exif_data.items():
                    tag_name = TAGS.get(tag_id, tag_id)
                    # Convert bytes to string for JSON serialization
                    if isinstance(value, bytes):
                        value = value.decode(errors="ignore")
                    metadata["exif"][str(tag_name)] = str(value)

        # 4. Define the output JSON path
        output_key = f"metadata/{os.path.basename(key)}.json"
        
        # 5. Upload JSON to S3
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
        
        print(f"Metadata written to {output_key}")

    return {'statusCode': 200, 'body': 'Metadata extraction complete'}
