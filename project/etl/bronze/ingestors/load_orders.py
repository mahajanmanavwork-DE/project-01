import s3fs 
import os 
import dotenv
from pyspark.sql.functions import col
import json 
dotenv.load_dotenv('/opt/spark/env/.env')

def load_order_data(spark,date):
    try:
        fs = s3fs.S3FileSystem(
            endpoint_url='http://minio:9000',
            key = os.getenv('MINIO_ROOT_USER'),
            secret = os.getenv("MINIO_ROOT_PASSWORD")
        )
        target_dir = 'data/raw/get_orders'
        all_files = fs.ls(target_dir)
        req_files = []
        for file in all_files:
            if str(date) in file:
                req_files.append(f's3a://{file}')
    except Exception as e:
        print(f'Error in orders while connection as {e}')
    else:
        if len(req_files) == 0:
            print('No files to write')
        else:
            orders_df = spark.read.format('json')\
                            .option('multiline',True)\
                            .load(req_files)
            
            invalid_orders = orders_df.filter(col('oid').isNull())
            valid_orders = orders_df.filter(col('oid').isNotNull())
            invalid_order_count = invalid_orders.count()
            orders_count = orders_df.count()
            
            if invalid_order_count >= orders_count*0.3:
                print(f'Unable to load data in orders table because of {invalid_order_count} invalid records')
            else:
                invalid_orders.write.format('delta').mode("append").option('delta.enableChangeDataFeed', 'true').save('s3a://data//quarantine//orders//')
                valid_orders.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//bronze//orders//')
                print('Data written for orders is successful')
                
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['bronze']["orders"] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()