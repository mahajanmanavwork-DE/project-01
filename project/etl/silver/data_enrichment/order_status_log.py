import s3fs 
import os 
import dotenv 
import json
from pyspark.sql.functions import col 
from pyspark.sql.types import TimestampType
import datetime
dotenv.load_dotenv('/opt/spark/env/.env')

def cleaned_order_staus_logs(spark):
    try:
        # Loading current version for bronze_layer and silver layer 
        lastest_bronze_order_staus_version = -1
        latest_silver_order_status_version = -1
        with open('/opt/spark/etl/versions.json','r') as f:
            versions = json.load(f)
            lastest_bronze_order_staus_version = versions['schema']['bronze']['order_status']
            latest_silver_order_status_version = versions['schema']['silver']['order_status']
            
            
        # Comparing versions to resolve correctly
        if latest_silver_order_status_version > lastest_bronze_order_staus_version:
            print('Issue in your versions ')
        elif latest_silver_order_status_version == lastest_bronze_order_staus_version:
            print('Data is already updated')
        else:
            for _ in range(lastest_bronze_order_staus_version - latest_silver_order_status_version):
                latest_silver_order_status_version +=1
                order_status_table = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', latest_silver_order_status_version).load('s3a://data//bronze//order_status_logs//')
                order_status_df = order_status_table.drop("_change_type","_commit_version","_commit_timestamp")
                
                # Logic for validating users_df for silver layer 
                order_status_df.cache()
                orders_df = spark.read.format('delta').load('s3a://data//silver//orders//')
                main_df = order_status_df.join(orders_df, order_status_df.order_id == orders_df.oid , 'left_semi')\
                    .fillna({"old_status":"Unknown","new_status":"Unknown","changed_by":"Unknown","remarks":"N/A"})\
                    .withColumn("changed_at",col("changed_at").cast(TimestampType()))
                
                try:
                    all_logs_df = spark.read.format('delta').load('s3a://data//silver//logs//')
                    final_df = main_df.join(all_logs_df, main_df.log_id == all_logs_df,'left_anti')
                except:
                    final_df = main_df
                finally:
                    invalid_df = order_status_df.join(final_df,order_status_df.log_id == final_df.log_id, 'left_anti')
                    
                    final_df.write.format('delta').mode('append').save('s3a://data//silver//order_status_logs//')
                    invalid_df.write.format('delta').mode('append').save('s3a://data//quarantine//order_status_logs//')
                    
                    order_status_df.unpersist()
                    # Updating version after each computation
                    with open('/opt/spark/etl/versions.json','r+') as f:
                        data = json.load(f)
                        data['schema']['silver']["order_status"] +=1
                        f.seek(0)
                        json.dump(data,f,indent=4)
                        f.truncate()
                    print('Updated')
                print('Done')
            
    except Exception as e:
        # Catching any unexpected errors
        print(f'Error is {e}')