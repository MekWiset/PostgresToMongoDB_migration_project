import pandas as pd
from pymongo import MongoClient
from typing import Optional


def rearrange_product(product_dict: Optional[dict]) -> Optional[dict]:
        
    if pd.notnull(product_dict) and isinstance(product_dict, dict):
        desired_order = ['unspscId', 'qty', 'productName', 'unitName', 'priceUnit']
        rearranged_dict = {key: product_dict[key] for key in desired_order if key in product_dict}
        remaining_fields = [key for key in product_dict if key not in desired_order]
        rearranged_dict.update({key: product_dict[key] for key in remaining_fields})
        return rearranged_dict
    else:
        return product_dict


def upload_to_mongodb(file_path: str, client_uri: str, dbname: str, collection_name: str) -> None:
    
    df_to_upload = pd.read_csv(file_path)
    
    if 'product' in df_to_upload.columns:
        df_to_upload['product'] = df_to_upload['product'].apply(rearrange_product)

    client = MongoClient(client_uri)
    db = client[dbname]
    collection = db[collection_name]

    df_json = df_to_upload.to_dict(orient='records')
    
    try:
        collection.insert_many(df_json)
    except Exception as e:
        print(f"An error occurred: {e}")
    
    client.close()

