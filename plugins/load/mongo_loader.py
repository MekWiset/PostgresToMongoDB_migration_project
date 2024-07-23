import pandas as pd
from pymongo import MongoClient
from typing import Optional
import os
from dotenv import load_dotenv
from pathlib import Path

from plugins.utils.timestamp_manager import timestampManager

timestamp_path = 'data/timestamp.txt'
tsmanager = timestampManager(timestamp_path)


def rearrange_product(product_dict: Optional[dict]) -> Optional[dict]:
    '''Rearrange fields in product dictionary.'''
    if pd.notnull(product_dict) and isinstance(product_dict, dict):
        desired_order = ['unspscId', 'qty', 'productName', 'unitName', 'priceUnit']
        rearranged_dict = {key: product_dict[key] for key in desired_order if key in product_dict}
        remaining_fields = [key for key in product_dict if key not in desired_order]
        rearranged_dict.update({key: product_dict[key] for key in remaining_fields})
        return rearranged_dict
    else:
        return product_dict


def upload_to_mongodb(file_path: str, client_uri: str, dbname: str, collection_name: str) -> None:
    '''Upload data from CSV to MongoDB collection.'''
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

    tsmanager.update_timestamp()


if __name__ == '__main__':

    dotenv_path = Path('.env')
    load_dotenv(dotenv_path = dotenv_path)

    file_path = 'data/medq_data_transformed.csv'
    client_uri = os.getenv('MONGO_CLIENT')
    db = 'HDX'
    collection = 'RMCPLUS_HDX_PRODUCT'

    upload_to_mongodb(file_path=file_path, client_uri=client_uri, dbname=db, collection_name=collection)

