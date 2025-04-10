import json
import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Get bucket and object key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Fetch the Excel file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()

        # Load Excel file using pandas
        excel_file = BytesIO(file_content)
        df = pd.read_excel(excel_file, engine='openpyxl')

        # Log DataFrame (or process as needed)
        print(f"Data from {key} in {bucket}:")
        print(df.head())  # Just print the first few rows

        return {
            'statusCode': 200,
            'body': json.dumps('Excel file processed successfully')
        }

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
