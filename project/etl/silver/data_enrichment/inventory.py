import s3fs 
import os 
import dotenv 
import json
from pyspark.sql.functions import col,desc,row_number,lit,abs
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
import datetime
dotenv.load_dotenv('/opt/spark/env/.env')

def cleaned_inventory(spark):
    try:
        # Loading current version for bronze_layer and silver layer 
        lastest_bronze_inventory_version = -1
        latest_silver_inventory_version = -1
        with open('/opt/spark/etl/versions.json','r') as f:
            versions = json.load(f)
            lastest_bronze_inventory_version = versions['schema']['bronze']['inventory']
            latest_silver_inventory_version = versions['schema']['silver']['inventory']
            
            
        # Comparing versions to resolve correctly
        if latest_silver_inventory_version > lastest_bronze_inventory_version:
            print('Issue in your versions ')
        elif latest_silver_inventory_version == lastest_bronze_inventory_version:
            print('Data is already updated')
        else:
            for _ in range(lastest_bronze_inventory_version - latest_silver_inventory_version):
                latest_silver_inventory_version +=1
                inventory_table = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', latest_silver_inventory_version).load('s3a://data//bronze//inventory//')
                inventory_df = inventory_table.drop("_change_type","_commit_version","_commit_timestamp")
                
                # Logic for validating users_df for silver layer 
                inventory_df.cache()
                product_df = spark.read.format('delta').load('s3a://data//silver//products//')
                
                main_df = inventory_df.join(product_df, inventory_df.product_id == product_df.pid, 'left_semi')\
                    .filter((col('quantity_on_hand') >= 0) & (col('reorder_threshold') >= 0))\
                    .withColumn("last_restocked_at",col('last_restocked_at').cast(DateType()))\
                    .fillna({"warehouse_location":"N/A"})
                    
                try:
                    all_inventory = spark.read.format('delta').load('s3a://data//silver//inventory//')
                    final_df = main_df.join(all_inventory, main_df.inventory_id == all_inventory.inventory_id, 'left_anti' )
                    final_df.write.format('delta').mode('append').save('s3a://data//silver//inventory//')
                except:
                    final_df = main_df
                    final_df.write.format('delta').mode('append').save('s3a://data//silver//inventory//')
                finally:
                    
                    invalid_df = inventory_df.join(final_df, inventory_df.inventory_id == final_df.inventory_id, 'left_anti')
                    invalid_df.write.format('delta').mode('append').save('s3a://data//quarantine//inventory//')

                    inventory_df.unpersist()
                    
                    # Updating version after each computation
                    with open('/opt/spark/etl/versions.json','r+') as f:
                        data = json.load(f)
                        data['schema']['silver']["inventory"] +=1
                        f.seek(0)
                        json.dump(data,f,indent=4)
                        f.truncate()
                    print('Updated')
                print('Done')
                
            
    except Exception as e:
        # Catching any unexpected errors
        print(f'Error is {e}')