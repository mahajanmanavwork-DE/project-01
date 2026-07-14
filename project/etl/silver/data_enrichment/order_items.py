import s3fs 
import os 
import dotenv 
import json
from pyspark.sql.functions import col,desc,row_number,lit # type: ignore
from pyspark.sql.window import Window # type: ignore
import datetime
dotenv.load_dotenv('/opt/spark/env/.env')

def cleaned_order_items(spark):
    try:
        # Loading current version for bronze_layer and silver layer 
        lastest_bronze_order_items_version = -1
        latest_silver_order_items_version = -1
        with open('/opt/spark/etl/versions.json','r') as f:
            versions = json.load(f)
            lastest_bronze_order_items_version = versions['schema']['bronze']['order_items']
            latest_silver_order_items_version = versions['schema']['silver']['order_items']
            
            
        # Comparing versions to resolve correctly
        if latest_silver_order_items_version > lastest_bronze_order_items_version:
            print('Issue in your versions ')
        elif latest_silver_order_items_version == lastest_bronze_order_items_version:
            print('Data is already updated')
        else:
            for _ in range(lastest_bronze_order_items_version - latest_silver_order_items_version):
                latest_silver_order_items_version +=1
                order_items_table = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', latest_silver_order_items_version).load('s3a://data//bronze//order_items//')
                order_items_df = order_items_table.drop("_change_type","_commit_version","_commit_timestamp")
                
                order_items_df.cache()
                
                # Logic for validating orders_df for silver layer 
                
                cols = ["order_item_id","order_id","product_id","quantity","unit_price_at_order","discount_at_order","line_total"]
                
                product_table = spark.read.format('delta').load('s3a://data//silver/products')
                valid_items = order_items_df.join(product_table,order_items_df['product_id'] == product_table['pid'],'left_semi' )\
                    .filter((col('quantity') > 0) | (col('unit_price_at_order') > 0 ) | (col('discount_at_order') >=0 ))\
                        .select(cols)
                try:
                    all_items = spark.read.format('delta').load('s3a://data//silver//order_items')
                    valid_items.join(all_items,valid_items['order_item_id'] == all_items['order_item_id'],'left_anti')\
                        .write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//silver//order_items')
                except:
                    valid_items.write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//silver//order_items')

                invalid_items = order_items_df.join(product_table,order_items_df['product_id'] == product_table['pid'],'left' )\
                    .filter((col('pid').isNull()) | (col('quantity') <= 0) | (col('unit_price_at_order') <= 0 ) | (col('discount_at_order') < 0 ))\
                        .select(cols)
                invalid_items.write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//quarantine//order_items')
                try:
                    all_items = spark.read.format('delta').load('s3a://data//silver//order_items')
                    valid_items.join(all_items,valid_items['order_item_id'] == all_items['order_item_id'],'left_semi')\
                        .write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//quarantine//order_items')
                except:
                    pass 
                
                order_items_df.unpersist()
                    
                # Updating version after each computation
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['silver']["order_items"] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
                print('Updated')
            print('Done')
            
    except Exception as e:
        # Catching any unexpected errors
        print(f'Error is {e}')
