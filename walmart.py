import json
from bson.json_util import dumps
from uploadImagefromUri import uploadImagefromUri
from utlity import insert_to_walmart_mongo, generate_gtin
from walmart_json_template import food_wrap, disposable_CUTLERY, paper_towal, paper_cups, paper_Plates

def createWalmartProductjson(df, bucket):
    products = []  # Initialize an empty list to store all products
    for index, row in df.iterrows():
        print(f"Processing row {index + 1}...")
        gtin = generate_gtin()

        if row["Category"] == "FOODWRAP":
            product = food_wrap
            product["form"] = ["Sheets"]
            product["ProductType"] = "Food Wraps"
            product["size"] = f'{row["Length"]}" x {row["Width"]}"'
        elif row["Category"] == "CUTLERY":
            product = disposable_CUTLERY
            product["ProductType"] = "Disposable Cutlery Sets"
            product["assembledProductWidth"] = {
                "measure": row["Width"],
                "unit": "in"
            }
            product["assembledProductLength"] = {
                "measure": row["Length"],
                "unit": "in"
            }
            product["assembledProductHeight"] = {
                "measure": row["Height"],
                "unit": "in"
            }
            product["assembledProductWeight"] = {
                "measure": row["Alternate UOM Net Weight"],
                "unit": "lb"
            }
            product["colorCategory"] = [row["Color"]]
            product["color"] = row["Color"]
        elif row["Category"] == "TOWEL":
            product = paper_towal
            product["form"] = ["Sheets"]
            product["ProductType"] = "Paper Towels"
            product["numberOfSheets"] = 1
            product["perforated"] = "No"
        elif row["Category"] == "CUPS":
            product = paper_cups
            product["ProductType"] = "Paper Cups"
            product["assembledProductWidth"] = {
                "measure": row["Width"],
                "unit": "in"
            }
            product["assembledProductLength"] = {
                "measure": row["Length"],
                "unit": "in"
            }
            product["assembledProductHeight"] = {
                "measure": row["Height"],
                "unit": "in"
            }
            product["assembledProductWeight"] = {
                "measure": row["Alternate UOM Net Weight"],
                "unit": "lb"
            }
            product["volumeCapacity"] = {
                "measure": row["Capacity"].replace(" OZ", ""),
                "unit": "oz"
            }
            product["colorCategory"] = [row["Color"]]
            product["color"] = row["Color"]
        elif row["Category"] == "PLATES":
            product = paper_Plates
            product["ProductType"] = "Paper Plates"
            product["assembledProductWidth"] = {
                "measure": row["Width"],
                "unit": "in"
            }
            product["assembledProductLength"] = {
                "measure": row["Length"],
                "unit": "in"
            }
            product["assembledProductHeight"] = {
                "measure": row["Height"],
                "unit": "in"
            }
            product["assembledProductWeight"] = {
                "measure": row["Alternate UOM Net Weight"],
                "unit": "lb"
            }
            product["colorCategory"] = [row["Color"]]
            product["color"] = row["Color"]

        product["sku"] = f"Sku-{str(gtin)}"
        product["ProductId"] = str(gtin)
        product["price"] = row.get("price", 100)
        product["productName"] = row["Category"]
        product["brand"] = row["Brand"]
        product["shortDescription"] = row["Product Title - en-US"]
        product["keyFeatures"] = [
            row["Features - en-US"],
            row["Features - en-US1"],
            row["Features - en-US2"],
            row["Features - en-US3"]
        ]
        product["material"] = row["Material Description"]
        product["mainImageUrl"] = row["Image 1"]
        product["manufacturer"] = row["Manufacturer Name"]
        product["productSecondaryImageURL"] = [
            row["Image 1"],
            row["Image 2"],
            row["Image 3"]
        ]
        product["netContent"] = {
            "productNetContentMeasure": row.get("Items Per Each", 1),
            "productNetContentUnit": "each"
        }

        # Append the product to the products list
        products.append(product)

    # Move this block outside the loop
    json_data = {
        "Items": products
    }
   
    
    
    insert_to_walmart_mongo(json_data, bucket)

    print("JSON file 'walmart_products.json' created successfully.")
    print(f"Posted to Mongo.")