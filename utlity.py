from pymongo import MongoClient
import random
import boto3
import requests
from io import BytesIO
from urllib.parse import urlparse
import os 




def insert_to_shopify_mongo(data, bucket):
    client = None
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://nuadmin:H9ck668ixt3!@44.211.106.255:19041/")
        mongodb = bucket.split('-')[0]
        db = client[mongodb]

        # Access the configuration collection
        configcollection = db[mongodb + "_Configuration"]
        configuration = configcollection.find_one()
        keys = set(configuration.keys())
      
        # Check if SHOPIFY_PRODUCT_COLLECTION exists
        if "SHOPIFY_PRODUCT_COLLECTION" in keys:
            print("In Shopify")
            collection = db[configuration["SHOPIFY_PRODUCT_COLLECTION"]]
            collection.insert_many(data)
            print("Data inserted into Shopify MongoDB successfully.")
    except Exception as e:
        # Handle exceptions and log the error
        print(f"An error occurred while inserting data into Shopify MongoDB: {str(e)}")
    finally:
        # Ensure the MongoDB client is closed
        if client:
            client.close()
            print("MongoDB connection closed.")

def insert_to_lazada_mongo(data, bucket):
    client = None
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://nuadmin:H9ck668ixt3!@44.211.106.255:19041/")
        mongodb =  bucket.split('-')[0]
        db = client[mongodb]

        # Access the configuration collection
        configcollection = db[mongodb + "_Configuration"]
        configuration = configcollection.find_one()
        keys = set(configuration.keys())
      
        # Check if SHOPIFY_PRODUCT_COLLECTION exists
        if "LAZADA_PRODUCT_COLLECTION" in keys:
            print("In lazada")
            collection = db[configuration["LAZADA_PRODUCT_COLLECTION"]]
            collection.insert_many(data)
            print("Data inserted into lazada MongoDB successfully.")
    except Exception as e:
        # Handle exceptions and log the error
        print(f"An error occurred while inserting data into lazada MongoDB: {str(e)}")
    finally:
        # Ensure the MongoDB client is closed
        if client:
            client.close()
            print("MongoDB connection closed.")

def insert_to_walmart_mongo(data, bucket):
    client = None
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://nuadmin:H9ck668ixt3!@44.211.106.255:19041/")
        mongodb =  bucket.split('-')[0]
        db = client[mongodb]

        # Access the configuration collection
        configcollection = db[mongodb + "_Configuration"]
        configuration = configcollection.find_one()
        keys = set(configuration.keys())

        # Check if WALMART_PRODUCT_COLLECTION exists
        if "WALMART_PRODUCT_COLLECTION" in keys:
            print("In Walmart")
            collection = db[configuration["WALMART_PRODUCT_COLLECTION"]]
            collection.insert_one(data)
            print("Data inserted into Walmart MongoDB successfully.")
    except Exception as e:
        # Handle exceptions and log the error
        print(f"An error occurred while inserting data into Walmart MongoDB: {str(e)}")
    finally:
        # Ensure the MongoDB client is closed
        if client:
            client.close()
            print("MongoDB connection closed.")
   
def generate_gtin():
    gtin = [str(random.randint(0, 9)) for _ in range(13)]
    check_digit = calculate_check_digit(gtin)
    gtin.append(str(check_digit))
    return ''.join(gtin)

def calculate_check_digit(gtin):
    total = 0
    for i, digit in enumerate(reversed(gtin)):
        total += int(digit) * (3 if i % 2 == 0 else 1)
    check_digit = (10 - (total % 10)) % 10
    return check_digit


def uploadImagefromUri(imageurl, bucket):
    imageurl = str(imageurl).strip()
    print(imageurl)
    if imageurl is None or imageurl == "" or imageurl == "nan":
        return ""
  
    print(imageurl)
    s3_key = get_image_name_from_url(imageurl)

    # Initialize the S3 client
    s3 = boto3.client(
        "s3",
         aws_access_key_id=os.getenv('MY_AWS_ACCESS_KEY'),
         aws_secret_access_key=os.getenv('MY_AWS_SECRET_KEY'),
    )

    # Check if the file already exists in the bucket
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        print(f"File with key '{s3_key}' already exists in bucket '{bucket}'.")
        return s3_key  # Return the key if the file exists
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"File with key '{s3_key}' does not exist. Uploading...")
        else:
            print(f"An error occurred: {e}")
            raise

    # Download the image from the URL
    response = requests.get(imageurl)
    if response.status_code == 200:
        image_data = BytesIO(response.content)
        # Upload the image to the bucket
        s3.upload_fileobj(image_data, bucket, s3_key)
        print(f"File with key '{s3_key}' uploaded successfully.")
    else:
        print(f"Failed to download image from URL: {imageurl}")
        return ""

    return s3_key

def get_image_name_from_url(image_url):
    # Parse the URL
    parsed_url = urlparse(image_url)
    # Extract the image name from the path
    image_name = parsed_url.path.split("/")[-1]
    return image_name

