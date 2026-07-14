import s3fs 
import os 
import dotenv 
import json
from pyspark.sql.functions import row_number,col,desc,lit # type: ignore
from pyspark.sql.window import Window # type: ignore
import datetime
dotenv.load_dotenv('/opt/spark/env/.env')

def cleaned_products(spark):
    try:
        # Loading current version for bronze_layer and silver layer 
        lastest_bronze_product_version = -1
        latest_silver_product_version = -1
        with open('/opt/spark/etl/versions.json','r') as f:
            versions = json.load(f)
            lastest_bronze_product_version = versions['schema']['bronze']['products']
            latest_silver_product_version = versions['schema']['silver']['products']
        # Comparing versions to resolve correctly
        if latest_silver_product_version > lastest_bronze_product_version:
            print('Issue in your versions ')
        elif latest_silver_product_version == lastest_bronze_product_version:
            print('Data is already updated')
        else:
            for _ in range(lastest_bronze_product_version - latest_silver_product_version):
                latest_silver_product_version +=1
                prd_df = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', latest_silver_product_version).option('endingVersion',latest_silver_product_version).load('s3a://data//bronze//products//')
                product_df = prd_df.drop("_change_type","_commit_version","_commit_timestamp").dropDuplicates()
                # Logic for validating users_df for silver layer 
                
                cols = ["pid","product_name","category","sub_category","brand","base_price","discount_price","current_stock","supplier","created_at"]
                try:
                    all_products_df = spark.read.format('delta').load('s3a://data//silver//products')
                    all_prod = all_products_df.count()
                   
                    df = product_df.join(all_products_df, all_products_df['pid'] == product_df['pid'], 'left_anti' )\
                        .select(cols).fillna({"product_name":"N/A","category":"N/A","sub_category":"N/A","brand":"N/A","supplier":"N/A"})\
                            .filter((col('base_price') > 0) & (col('current_stock') >= 0) )
        
                    df.write.format('delta').mode('append').option('delta.enableChangeDataFeed',True).save('s3a://data//silver//products')
                    df_cnt = df.count()
                    
                    invalid_df = product_df.join(all_products_df, all_products_df['pid'] == product_df['pid'], 'left_semi' )\
                        .select(cols)
                    invalid_df.write.format('delta').mode('append').option('delta.enableChangeDataFeed',True).save('s3a://data//quarantine//products')
                    invalid_cnt = invalid_df.count()
                    print(f'df_count as {df_cnt}')
                    print(f'all_prod count as {all_prod}')
                    print(f'invalid_df as {invalid_cnt}')
                except:
                    df = product_df.fillna({"product_name":"N/A","category":"N/A","sub_category":"N/A","brand":"N/A","supplier":"N/A"})\
                            .filter((col('base_price') > 0) & (col('current_stock') >= 0) )
                    
                    df.write.format('delta').mode('append').option('delta.enableChangeDataFeed',True).save('s3a://data//silver//products')
                    df_cnt = df.count()
                    print(f'df_count of except as {df_cnt}')
                # Updating version after each computation
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['silver']["products"] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
                print('Updated')
            print('Done')
    except Exception as e:
        # Catching any unexpected errors
        print(f'Error is {e}')