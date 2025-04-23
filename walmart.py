import json
from bson.json_util import dumps
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
            product["size"] = f'{round(row["Length"],2)}" x {round(row["Width"],2)}"'
        elif row["Category"] == "CUTLERY":
            product = disposable_CUTLERY
            product["ProductType"] = "Disposable Cutlery Sets"
            product["assembledProductWidth"] = {
                "measure": round(row["Width"],2),
                "unit": "in"
            }
            product["assembledProductLength"] = {
                "measure": round(row["Length"],2),
                "unit": "in"
            }
            product["assembledProductHeight"] = {
                "measure": round(row["Height"],2),
                "unit": "in"
            }
            product["assembledProductWeight"] = {
                "measure": round(row["Alternate UOM Net Weight"],2),
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
                "measure": round(row["Width"],2),
                "unit": "in"
            }
            product["assembledProductLength"] = {
                "measure": round(row["Length"],2),
                "unit": "in"
            }
            product["assembledProductHeight"] = {
                "measure": round(row["Height"],2),
                "unit": "in"
            }
            product["assembledProductWeight"] = {
                "measure": round(row["Gross Weight"],2),
                "unit": "lb"
            }
            product["volumeCapacity"] = {
                "measure": int(row["Capacity"].replace(" OZ", "").strip()),
                "unit": "oz"
            }
            product["colorCategory"] = [row["Color"]]
            product["color"] = row["Color"]
        elif row["Category"] == "PLATES":
            product = paper_Plates
            product["ProductType"] = "Paper Plates"
            product["assembledProductWidth"] = {
                "measure": round(row["Width"],2),
                "unit": "in"
            }
            product["assembledProductLength"] = {
                "measure": round(row["Length"],2),
                "unit": "in"
            }
            product["assembledProductHeight"] = {
                "measure": round(row["Height"],2),
                "unit": "in"
            }
            product["assembledProductWeight"] = {
                "measure": round(row["Alternate UOM Net Weight"],2),
                "unit": "lb"
            }
            product["colorCategory"] = [row["Color"]]
            product["color"] = row["Color"]

        product["sku"] = f"Sku-{str(gtin)}"
        product["ProductId"] = str(gtin)
        
        product["price"] = row.get("price", 100)
        print(row["Color"])
        print(row["Color"]+"-"+row["Category"])
        product["productName"] = row["Color"]+" "+row["Category"]
        product["brand"] = row["Brand"]
        print("1")
        product["shortDescription"] = row["Product Title - en-US"]
        product["keyFeatures"] = [
            row["Features - en-US"],
            row["Features - en-US1"],
            row["Features - en-US2"],
            row["Features - en-US3"]
        ]
        product["material"] = ["Plastic","Waxed Paper","Wood","Dry Waxed Paper"]
        product["mainImageUrl"] = row["Image 1"]
        product["manufacturer"] = row["Manufacturer Name"]
        product["manufacturerPartNumber"]=row["Material Number"]
        product["productSecondaryImageURL"] = [
            row["Image 1"],
            row["Image 2"],
            row["Image 3"],
            row["Image 1"]
        ]
        product["netContent"] = {
            "productNetContentMeasure": row.get("Items Per Each", 1),
            "productNetContentUnit": "Each"
        }
        product["ShippingWeight"]=round(row.get("Gross Weight", 1.20),2)

        # Append the product to the products list
        products.append(product)

    # Move this block outside the loop
    json_data = {
        "Items": products
    }
   
    
    insert_to_walmart_mongo(json_data, bucket)

    print("JSON file 'walmart_products.json' created successfully.")
    print(f"Posted to Mongo.")