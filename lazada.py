import json
from pymongo import MongoClient
from bson.json_util import dumps

from utlity import  uploadImagefromUri,insert_to_lazada_mongo

# import os
# os.makedirs("lazadaJson", exist_ok=True)


def createLazadaProductjson(df, bucket):
    products = []
    productjson = ""
    for index, row in df.iterrows():

        product = {
            "Request": {
                "Product": {
                    "PrimaryCategory": "CUPS",  # p  Pending
                    "Images": {
                        "Image": []
                    },
                    "Attributes": {
                        "name": "",  # category
                        "disableAttributeAutoFill": False,
                        "description": "16 oz. Paper Hot Cups",  ## Product Title - en-US
                        "brand_name": "Georgia-Pacific NA Commercial",  # Brand
                        "model": "UHD-HC-2M",  # not mapped
                        "waterproof": "Not Waterproof",  # not mapped
                        "warranty_type": "Local seller warranty",  # not mapped
                        "warranty": "1 Month",  # not mapped
                        "short_description": "Dixie Cup",  # JV
                        "Hazmat": "None",  # not mapped
                        "material": "CUPS, PPR 16OZ HC 20/50 PATH",  # AT
                        "laptop_size": "Not Applicable",  # not mapped
                        "delivery_option_sof": "No",  # not mapped
                        "gift_wrapping": "No",  # not mapped
                        "name_engravement": "No",  # not mapped
                        "preorder_enable": "No",  # not mapped
                        "preorder_days": "0",  # not mapped
                    },
                    "Skus": {
                        "Sku": [
                            {
                                "SellerSku": "SKU10078731940541",  # A
                                "quantity": "6",  # JF
                                "price": "30",  # LG
                                "package_height": "22.125",  # AM
                                "package_length": "18.313",  # AR
                                "package_width": "14.688",  # CO
                                "package_weight": "27.711",  # AK
                                "package_content": "50 x UltraHD 4K HDMI 2.1 Cable (2M)",  # AY
                                "Images": {
                                    "Image": []
                                },
                            }
                        ]
                    },
                }
            }
        }
        
         

        image1=uploadImagefromUri(row["Image 1"],bucket)
        image2=uploadImagefromUri(row["Image 2"],bucket)
        image3=uploadImagefromUri(row["Image 3"],bucket)
        imagedefault = imagedefault if image1==""else image1
        product["Request"]["Product"]["Images"]["Image"] = [
            imagedefault if image1==""else image1,
            imagedefault if image2==""else image2,
            imagedefault if image3==""else image3
        ]
        
        product["Request"]["Product"]["Skus"]["Sku"][0]["Images"]["Image"] = [
            imagedefault if image1==""else image1,
            imagedefault if image2==""else image2,
            imagedefault if image3==""else image3
        ]
        
        product["Request"]["Product"]["Attributes"]["name"] = row["Category"].title()
        product["Request"]["Product"]["Attributes"]["description"] = row["Product Title - en-US"].title()
        product["Request"]["Product"]["Attributes"]["short_description"] = row["Material Description"].title()
        product["Request"]["Product"]["Attributes"]["Hazmat"] = "None"
        product["Request"]["Product"]["Attributes"]["material"] = row["Material Description"].title()
        
        
        product["Request"]["Product"]["Skus"]["Sku"][0]["SellerSku"]='Sku-'+str(row["GTIN"])
        product["Request"]["Product"]["Skus"]["Sku"][0]["quantity"]=10
        product["Request"]["Product"]["Skus"]["Sku"][0]["price"]=row["price"]
        product["Request"]["Product"]["Skus"]["Sku"][0]["package_height"]=row["Height"]
        product["Request"]["Product"]["Skus"]["Sku"][0]["package_length"]=row["Length"]
        product["Request"]["Product"]["Skus"]["Sku"][0]["package_width"]=row["Width"]
        product["Request"]["Product"]["Skus"]["Sku"][0]["package_weight"]=row["Gross Weight"] if row["Gross Weight"]<=300 else 300
        product["Request"]["Product"]["Skus"]["Sku"][0]["package_content"]=row["Material Description"]
        
        if(row["Category"] == "FOODWRAP"):
            product["Request"]["Product"]["PrimaryCategory"] = "Food Warmers"
            product["Request"]["Product"]["Attributes"]["brand_name"] = "Benro"
        elif(row["Category"] == "CUTLERY"):
            product["Request"]["Product"]["PrimaryCategory"] = "Knives"
            product["Request"]["Product"]["Attributes"]["brand_name"] = "Fridge-To-Go"
        elif(row["Category"] == "TOWEL"):
            product["Request"]["Product"]["PrimaryCategory"] = "Towels"
            product["Request"]["Product"]["Attributes"]["brand_name"] = "Caring"
        elif(row["Category"] == "CUPS"):
            product["Request"]["Product"]["PrimaryCategory"] = "Bottom Cups"
            product["Request"]["Product"]["Attributes"]["brand_name"] = "Maglite"
        elif(row["Category"] == "PLATES"):
            product["Request"]["Product"]["PrimaryCategory"] = "Plates"
            product["Request"]["Product"]["Attributes"]["brand_name"] = "Pure Harvest"
        
        products.append(product)
        #insert_to_lazada_mongo(products, bucket)
        #products=[]
        #with open(f"lazadaJson\\lazada_products{index}{row["Category"]}.json", "w", encoding="utf-8") as f:
            #json.dump(product, f, indent=4, ensure_ascii=False)
    insert_to_lazada_mongo(products, bucket)