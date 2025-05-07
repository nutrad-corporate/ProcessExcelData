import boto3
import requests
from io import BytesIO
from urllib.parse import urlparse
import os 
from dotenv import load_dotenv

load_dotenv()

def uploadImagefromUri(imageurl,bucket):
    imageurl = str(imageurl).strip()
    print(imageurl)
    if imageurl is None or imageurl == "" or imageurl == "nan":
        return ""
    print('a')
    print(imageurl)
    s3_key = get_image_name_from_url(imageurl)
    response = requests.get(imageurl)
    if response.status_code == 200:
        image_data = BytesIO(response.content)

        # Initialize the S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        )
        s3.upload_fileobj(image_data, bucket, s3_key)
    # Upload the image to the bucket
    return s3_key




def get_image_name_from_url(image_url):
    # Parse the URL
    parsed_url = urlparse(image_url)
    # Extract the image name from the path
    image_name = parsed_url.path.split("/")[-1]
    return image_name