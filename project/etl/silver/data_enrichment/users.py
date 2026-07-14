import s3fs 
import os 
import dotenv 
import json
from pyspark.sql.functions import col,desc,row_number,lit # type: ignore
from pyspark.sql.window import Window # type: ignore
import datetime

# Loading env variables 
dotenv.load_dotenv('/opt/spark/env/.env')

def cleaned_users(spark):
    try:
        # Loading current version for bronze_layer and silver layer 
        lastest_bronze_user_version = -1
        latest_silver_user_version = -1
        with open('/opt/spark/etl/versions.json','r') as f:
            versions = json.load(f)
            lastest_bronze_user_version = versions['schema']['bronze']['user_data']
            latest_silver_user_version = versions['schema']['silver']['user_data']
            
            
        # Comparing versions to resolve correctly
        if latest_silver_user_version > lastest_bronze_user_version:
            print('Issue in your versions ')
        elif latest_silver_user_version == lastest_bronze_user_version:
            print('Data is already updated')
        else:
            for _ in range(lastest_bronze_user_version - latest_silver_user_version):
                latest_silver_user_version +=1
                users_table = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', latest_silver_user_version).load('s3a://data//bronze//user_data//')
                user_df = users_table.drop("_change_type","_commit_version","_commit_timestamp").dropDuplicates(['customer_id'])
                
                # Logic for validating users_df for silver layer 
                try:
                    all_user_df = spark.read.format('delta').load('s3a://data//silver//user_data//').select('cust_id')
                    df = user_df.join(all_user_df,user_df['customer_id'] == all_user_df['cust_id'],'left_anti' )\
                        .select(user_df['customer_id'],user_df['name'],user_df['email'],user_df['city'],user_df['country'],user_df['created_on'])
                    df.write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//silver//user_data')
                
                    invalid_df = user_df.join(all_user_df,user_df['customer_id'] == all_user_df['cust_id'],'left_semi')\
                                .select(user_df['customer_id'],user_df['name'],user_df['email'],user_df['city'],user_df['country'],user_df['created_on'])
                    invalid_df.write.format('delta').mode('append').save('s3a://data//quarantine//user_data')
                
                except:
                    user_df.write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//silver//user_data//')
                
                
                # Updating version after each computation
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['silver']["user_data"] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
                print('Updated')
            print('Done')
            
    except Exception as e:
        # Catching any unexpected errors
        print(f'Error is {e}')