import s3fs
from pyspark.sql.functions import col
import os 
import dotenv
import json

dotenv.load_dotenv('/opt/spark/env/.env')

def load_user_data(spark,date):
    fs = s3fs.S3FileSystem(
        endpoint_url='http://minio:9000',
        key=os.getenv("MINIO_ROOT_USER"),
        secret=os.getenv("MINIO_ROOT_PASSWORD")
    )
    target_dir = "data/raw/get_user_data"
    try:
        all_files = fs.ls(target_dir)
        req_files = []
        for file in all_files:
            if str(date) in file:
                req_files.append(f's3a://{file}')
    except Exception as e:
        print(f'Error in user as {e}')
    else:
        user_df = spark.read.format('json')\
            .option('multiline',True)\
            .load(req_files)
            
        invalid_cid = user_df.filter(col('customer_id').isNull() )
        valid_cid = user_df.filter(col('customer_id').isNotNull() )
        
        user_df_count = user_df.count()
        invalid_record_count = invalid_cid.count()
        
        if invalid_record_count >= user_df_count * 0.3:
            print(f'Invalid records count {invalid_record_count}')
        else:
            invalid_cid.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//quarantine//user_data//')   
            valid_cid.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//bronze//user_data//') 
            print(f'Done extraction of user for {str(date)}')
        
            with open('/opt/spark/etl/versions.json','r+') as f:
                data = json.load(f)
                data['schema']['bronze']["user_data"] +=1
                f.seek(0)
                json.dump(data,f,indent=4)
                f.truncate()
                
                