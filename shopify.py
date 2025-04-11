import json
from pymongo import MongoClient
from bson.json_util import dumps
from uploadImagefromUri import uploadImagefromUri

def createShopifyProductjson(df,bucket):
   products = []
   productjson= ""; 
   for _, row in df.iterrows():
      
      product = {
         "title": row["Product Title - en-US"],
         "body_html": '<span><p>'+str(row["Full Description - en-US"])+'</p></span>',
         "vendor": "",
         "product_type": row["Category"],
         "variants": [
             {
              "price": 0,
              "option1": "Default",
              "inventory_management": "shopify",
              "requires_shipping": True,
              "sku": 'Sku-'+str(row["GTIN"]),
              "barcode": row["GTIN"],
              "weight": row["BUOM Net Weight"],
              "weight_unit": "lb",
              "inventory_quantity": 10,
             }
            ],
            "options": [
                {
                    "position": 2
                }
            ],
               "images": [
            {
               "src": uploadImagefromUri(row["Image 1"],bucket)
            },
            {
               "src":  uploadImagefromUri(row["Image 2"],bucket)
            },
            {
               "src":  uploadImagefromUri(row["Image 3"],bucket)
            }
      ]
      }
      products.append(product)
      insert_to_mongo(products,bucket)
      products=[]
      data = {
        "products": productjson
       }
   
def insert_to_mongo(data,bucket):
   client = MongoClient("mongodb://nuadmin:H9ck668ixt3!@44.211.106.255:19041/")
   mongodb = bucket.replace("-bucket", "")
   db = client[mongodb]
   collection=db[mongodb+"_Configuration"]
   document = collection.find_one()
   collection = db[f"{mongodb}_SHOPIFY_PRODUCT"]
   collection.insert_many(data)
   print("Data inserted into MongoDB successfully.")
   client.close()