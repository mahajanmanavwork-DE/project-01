import s3fs 
from pyspark.sql.functions import col
import os 
import dotenv
import json

dotenv.load_dotenv('/opt/spark/env/.env')
def load_product_data(spark,date):
    
    try:
        fs = s3fs.S3FileSystem(endpoint_url='http://minio:9000',
                               key=os.getenv('MINIO_ROOT_USER'),
                               secret=os.getenv('MINIO_ROOT_PASSWORD'))
        
        target_path = 'data/raw/get_products'
        all_files = fs.ls(target_path)
        req_file = []
        
        for file in all_files:
            if str(date) in file:
                req_file.append(f's3a://{file}')
            
        if len(req_file) == 0:
            print(f'No product is present for {date}')
        else:
            products_df = spark.read.format('json').option('multiline',True).load(req_file)
            invalid_pid = products_df.filter(col('pid').isNull())
            valid_pid = products_df.filter(col('pid').isNotNull())
            invalid_pid_count = invalid_pid.count()
            products_df_count = products_df.count()
            print(f'valid pid count is {valid_pid.count()}')
            print(f'invalid pid count is {invalid_pid_count}')
            print(f'products_df_count pid count is {products_df_count}')
            if invalid_pid_count > products_df_count*0.3:
                print(f'Unable to append any data in products due to {invalid_pid_count} invalid records')
            else:
                invalid_pid.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//quarantine//products')
                valid_pid.write.format('delta').mode('append').option('delta.enableChangeDataFeed', 'true').save('s3a://data//bronze//products')
                print('Data written for products success')
                
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['bronze']["products"] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
    except Exception as e:
        print(f'Error in products as {e}')