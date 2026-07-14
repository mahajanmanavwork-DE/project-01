from pyspark.sql.functions import col
import s3fs
import os 
import dotenv
import json 

dotenv.load_dotenv('/opt/spark/env/.env')

def load_order_status_log(spark,date):
    try:
        fs = s3fs.S3FileSystem(
            endpoint_url='http://minio:9000',
            key = os.getenv('MINIO_ROOT_USER'),
            secret= os.getenv('MINIO_ROOT_PASSWORD')
        )
        target_dir = 'data/raw/get_order_status_log'
        files = fs.ls(target_dir)
        req_files = []
        for file in files:
            if str(date) in file:
                req_files.append(f's3a://{file}')
    except Exception as e:
        print(f'Error occured as {e}')
    else:
        order_status_df = spark.read.format('json').option('multiline',True).load(req_files)
        valid_order_status_df = order_status_df.filter((col('log_id').isNotNull()) | (col('order_id').isNotNull()))
        invalid_order_status_df = order_status_df.filter((col('log_id').isNull()) & (col('order_id').isNotNull()))
        
        order_status_count = order_status_df.count()
        invalid_order_status_count = invalid_order_status_df.count()
        
        if invalid_order_status_count >= order_status_count*0.3:
            print(f'Unable to load order status data because of {invalid_order_status_count} invalid records')
        else:
            valid_order_status_df.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//bronze//order_status_logs//')
            invalid_order_status_df.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//quarantine//order_status_logs//')
            print('Data written success for order status logs ')
        
            with open('/opt/spark/etl/versions.json','r+') as f:
                data = json.load(f)
                data['schema']['bronze']["order_status"] +=1
                f.seek(0)
                json.dump(data,f,indent=4)
                f.truncate()