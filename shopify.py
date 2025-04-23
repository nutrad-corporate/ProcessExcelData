import json
from pymongo import MongoClient
from bson.json_util import dumps
from utlity import insert_to_shopify_mongo,uploadImagefromUri

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
              "price": row.get("price", 100),
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
      insert_to_shopify_mongo(products,bucket)
      products=[]
      data = {
        "products": productjson
       }
   
