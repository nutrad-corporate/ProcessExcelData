from pymongo import MongoClient
import random


def insert_to_shopify_mongo(data, bucket):
    client = None
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://nuadmin:H9ck668ixt3!@44.211.106.255:19041/")
        mongodb = bucket.replace("-bucket", "")
        db = client[mongodb]

        # Access the configuration collection
        collection = db[mongodb + "_Configuration"]
        configuration = collection.find_one()
        keys = set(configuration.keys())
      
        # Check if SHOPIFY_PRODUCT_COLLECTION exists
        if "SHOPIFY_PRODUCT_COLLECTION" in keys:
            print("In Shopify")
            collection = db[f"{mongodb}_SHOPIFY_PRODUCT"]
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

def insert_to_walmart_mongo(data, bucket):
    client = None
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://nuadmin:H9ck668ixt3!@44.211.106.255:19041/")
        mongodb = bucket.replace("-bucket", "")
        db = client[mongodb]

        # Access the configuration collection
        collection = db[mongodb + "_Configuration"]
        configuration = collection.find_one()
        keys = set(configuration.keys())

        # Check if WALMART_PRODUCT_COLLECTION exists
        if "WALMART_PRODUCT_COLLECTION" in keys:
            print("In Walmart")
            collection = db[f"{mongodb}_WALMART_PRODUCT"]
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