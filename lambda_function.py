import json
import boto3
import pandas as pd
from io import BytesIO
from shopify import createShopifyProductjson
from walmart import createWalmartProductjson
from lazada import createLazadaProductjson
from mapping import create_mapping
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        print("Lamda sterted")
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        archive_key = f"archive/{key.replace('.','')}"
        ensure_archive_directory(bucket)
        print(bucket)
        print(key)

        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()

        # Load Excel file using pandas
        excel_file = BytesIO(file_content)
        df = pd.read_excel(excel_file, engine='openpyxl')

        # Log DataFrame (or process as needed)
        print(f"Data from {key} in {bucket}:")
        print(df.head())  # Just print the first few rows

        if not key.startswith("CPG"):
            createShopifyProductjson(df,bucket)
            createWalmartProductjson(df,bucket)
            createLazadaProductjson(df,bucket)

        create_mapping(bucket,key)

        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=archive_key)
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"File moved to archive folder: {archive_key}")
        
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

def ensure_archive_directory(bucket):
    """Ensure the 'archive/' directory exists in the S3 bucket."""
    try:
        # Check if the archive directory exists
        s3.head_object(Bucket=bucket, Key="archive/")
    except s3.exceptions.ClientError as e:
        # If the directory does not exist, create it by uploading an empty object
        if e.response['Error']['Code'] == "404":
            s3.put_object(Bucket=bucket, Key="archive/")
            print("Archive directory created.")
        else:
            raise
