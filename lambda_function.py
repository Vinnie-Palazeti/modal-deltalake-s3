import json
import urllib3
import os

# thank you claude
def lambda_handler(event, context):
    # Get environment variables
    modal_url = os.getenv('MODAL_URL')
    modal_key = os.getenv('MODAL_KEY')
    modal_secret = os.getenv('MODAL_SECRET')
    
    if not all([modal_url, modal_key, modal_secret]):
        raise ValueError("Missing required environment variables")
    
    # Parse S3 event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Only process CSV files
        if not key.endswith('.csv'):
            continue
            
        # Prepare the payload
        payload = {
            "bucket": bucket,
            "key": key
        }
        
        # Make the POST request
        http = urllib3.PoolManager()
        
        headers = {
            'Modal-Key': modal_key,
            'Modal-Secret': modal_secret,
            'Content-Type': 'application/json'
        }
        
        try:
            response = http.request(
                'POST',
                modal_url,
                body=json.dumps(payload),
                headers=headers
            )
            
            print(f"Modal job triggered for {bucket}/{key}, response: {response.status}")
            
        except Exception as e:
            print(f"Error calling Modal endpoint: {str(e)}")
            raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed S3 event')
    }