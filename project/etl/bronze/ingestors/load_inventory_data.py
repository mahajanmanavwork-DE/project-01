import os 
import dotenv 
import s3fs 
from pyspark.sql.functions import col
import json

dotenv.load_dotenv('/opt/spark/env/.env') 

def load_inventory(spark,date):
    try:
        fs = s3fs.S3FileSystem(
            endpoint_url= 'http://minio:9000',
            key= os.getenv('MINIO_ROOT_USER'),
            secret= os.getenv('MINIO_ROOT_PASSWORD')
        )
        target_dir = '/data/raw/get_inventory'
        files = fs.ls(target_dir)
        req_files = []
        for file in files:
            if str(date) in file:
                req_files.append(f's3a://{file}')
    except Exception as e:
        print(f'Error occured in inventory as {e}') 
    else:
        inventory_df = spark.read.format('json').option('multiline',True).load(req_files)
        valid_inventory = inventory_df.filter((col('inventory_id').isNotNull()) & (col('product_id').isNotNull()))
        invalid_inventory = inventory_df.filter((col('inventory_id').isNull()) | (col('product_id').isNull()))
        
        inventory_count = inventory_df.count()
        invalid_inventory_count = invalid_inventory.count()
        
        if invalid_inventory_count >= inventory_count*0.3:
            print(f'Unable to load inventory data due to {invalid_inventory_count} invalid rows')
        else:
            valid_inventory.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//bronze//inventory')
            invalid_inventory.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//quarantine//inventory')
            print('Data written success for inventory data')
            
            with open('/opt/spark/etl/versions.json','r+') as f:
                data = json.load(f)
                data['schema']['bronze']["inventory"] +=1
                f.seek(0)
                json.dump(data,f,indent=4)
                f.truncate()