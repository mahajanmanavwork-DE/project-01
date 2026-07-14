import os 
import dotenv
import s3fs
from pyspark.sql.functions import col
import json

dotenv.load_dotenv('/opt/spark/env/.env')

def load_order_items(spark,date):
    try:
        fs = s3fs.S3FileSystem(
            endpoint_url='http://minio:9000',
            key = os.getenv('MINIO_ROOT_USER'),
            secret=os.getenv('MINIO_ROOT_PASSWORD')
        )
        
        target_dir = 'data/raw/get_order_items'
        
        files = fs.ls(target_dir)
        req_files = []
        for file in files:
            if str(date) in file:
                req_files.append(f's3a://{file}')
    except Exception as e:
        print(f'Error occured as {e}')
    else:
        if len(req_files) == 0:
            print('No order items files present to write ')
        
        else:
            order_items = spark.read.format('json').option('multiline',True).load(req_files)
            
            valid_orders = order_items.filter((col('order_item_id').isNotNull()) | (col('order_id').isNotNull()))
            
            invalid_orders = order_items.filter((col('order_item_id').isNull()) | (col('order_id').isNull()))
            
            invalid_order_cnt = invalid_orders.count()
            order_items_cnt = order_items.count()
            
            if invalid_order_cnt >= order_items_cnt*0.3:
                print(f'Unable to load data due to {invalid_order_cnt} invalid orders')
            else:
                valid_orders.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//bronze//order_items//')
                invalid_orders.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//quarantine//order_items//')
                print('Data written success for order items')
                
            with open('/opt/spark/etl/versions.json','r+') as f:
                data = json.load(f)
                data['schema']['bronze']["order_items"] +=1
                f.seek(0)
                json.dump(data,f,indent=4)
                f.truncate()
                    
