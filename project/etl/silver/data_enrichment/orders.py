import s3fs 
import os 
import dotenv 
import json
from pyspark.sql.functions import col,desc,row_number,lit,when,sum,avg # type: ignore
from pyspark.sql.window import Window # type: ignore
from pyspark.sql.types import DateType # type: ignore
import datetime
dotenv.load_dotenv('/opt/spark/env/.env')

def cleaned_orders(spark):
    try:
        # Loading current version for bronze_layer and silver layer 
        lastest_bronze_order_version = -1
        latest_silver_order_version = -1
        with open('/opt/spark/etl/versions.json','r') as f:
            versions = json.load(f)
            lastest_bronze_order_version = versions['schema']['bronze']['orders']
            latest_silver_order_version = versions['schema']['silver']['orders']
            
            
        # Comparing versions to resolve correctly
        if latest_silver_order_version > lastest_bronze_order_version:
            print('Issue in your versions ')
        elif latest_silver_order_version == lastest_bronze_order_version:
            print('Data is already updated')
        else:
            for _ in range(lastest_bronze_order_version - latest_silver_order_version):
                latest_silver_order_version +=1
                orders_table = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', latest_silver_order_version).load('s3a://data//bronze//orders//')
                new_orders = orders_table.drop("_change_type","_commit_version","_commit_timestamp").dropDuplicates()
                new_orders.cache()
                
                customer_df = spark.read.format('delta').load('s3a://data//silver/user_data//')
                
                clean_df_1 = new_orders.join(customer_df, new_orders['customer_id'] == customer_df['customer_id'], 'left_semi' )\
                    .withColumn('order_date',col('order_date').cast(DateType()))\
                    .fillna({"order_status":"Unknown","payment_method":"Unknown","shipping_address":"Unknown"})\
                
                order_items_df = spark.read.format('delta').load('s3a://data//silver/order_items')
                
                aggregated_df = new_orders.join(order_items_df, new_orders.oid == order_items_df.order_id, 'inner')\
                    .groupBy(new_orders.oid).agg(sum(order_items_df.unit_price_at_order * order_items_df.quantity).alias('total_amount')\
                        ,avg(order_items_df.discount_at_order).alias('discount_applied')\
                        ,sum(order_items_df.line_total).alias('final_amount'))
                try:
                    orders_df = spark.read.format('delta').load('s3a://data//silver//orders//')
                
                    final_df = clean_df_1.join(aggregated_df,clean_df_1.oid == aggregated_df.oid,'inner')\
                        .join(orders_df,clean_df_1.oid == orders_df.oid, 'left_anti')\
                        .select(clean_df_1.oid,clean_df_1.customer_id,clean_df_1.order_date,clean_df_1.order_status\
                        ,clean_df_1.payment_method,clean_df_1.shipping_address,aggregated_df.total_amount, aggregated_df.discount_applied,aggregated_df.final_amount)
                
                except:
                    final_df = clean_df_1.join(aggregated_df,clean_df_1.oid == aggregated_df.oid,'inner')\
                        .select(clean_df_1.oid,clean_df_1.customer_id,clean_df_1.order_date,clean_df_1.order_status\
                        ,clean_df_1.payment_method,clean_df_1.shipping_address,aggregated_df.total_amount, aggregated_df.discount_applied,aggregated_df.final_amount)
                     
                invalid_df = new_orders.join(final_df,final_df.oid == final_df.oid, 'left_anti')
                
                final_df.write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//silver//orders//')    
                invalid_df.write.format('delta').mode('append').option("delta.enableChangeDataFeed", "true").save('s3a://data//quarantine//orders//')  

                new_orders.unpersist()
                # Updating version after each computation
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['silver']["orders"] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
                print('Updated')
            print('Done')
            
    except Exception as e:
        # Catching any unexpected errors
        print(f'Error is {e}')