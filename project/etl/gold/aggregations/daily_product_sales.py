import sys 
sys.path.append('/opt/spark/')

from pyspark.sql import SparkSession # type: ignore 
from pyspark.sql.functions import col, lit,sum,round
from delta.tables import DeltaTable
import os 
import dotenv 
import json
from etl.silver.data_enrichment.products import cleaned_products


dotenv.load_dotenv('/opt/spark/env/.env')

spark = SparkSession.builder \
    .appName("BronzeLayerInbound") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ROOT_USER')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_ROOT_PASSWORD')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def daily_product_sales(spark):
    
    with open('/opt/spark/etl/versions.json','r') as f:
        data = json.load(f)
        
        silver_orders = data['schema']['silver']['orders']
        product_sales = data['schema']['gold']['daily_product_sales']
        
        if silver_orders > product_sales:
            for _ in range(silver_orders - product_sales):
                product_sales +=1
                print(f'product sales as of {product_sales}') 
                product_df = spark.read.format('delta').load('s3a://data//silver//products//')
                orders_df = spark.read.format('delta').option('readChangefeed',True).option('startingVersion',product_sales).option('endingversion',product_sales).load('s3a://data//silver//orders//')
                order_items_df = spark.read.format('delta').option('readChangefeed',True).option('startingVersion',product_sales).option('endingversion',product_sales).load('s3a://data//silver//order_items//')
                
                joined_df = orders_df.join(order_items_df,orders_df.oid == order_items_df.order_id, 'inner')
                
                df2 = joined_df.join(product_df, order_items_df.product_id == product_df.pid,'inner')\
                    .select(product_df.pid,product_df.product_name,product_df.category,product_df.sub_category,product_df.brand,product_df.base_price,orders_df.order_date, order_items_df.quantity, order_items_df.unit_price_at_order)
                
                result_df = df2.groupBy('pid','product_name','category','sub_category','brand','base_price','order_date')\
                    .agg(sum('quantity').alias('sales_count'), sum(col('quantity') * col('unit_price_at_order') ).alias('total_sales_amount'))
                
                result_df.write.format('delta').mode('append').save('s3a://data//gold//daily_product_sales//')
            
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                
                    data['schema']['gold']['daily_product_sales'] += 1 
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
                    
            gold_df = spark.read.format('delta').load('s3a://data//gold//daily_product_sales//')
            result_df_2 = gold_df.groupBy('pid','product_name','category','sub_category','brand','base_price','order_date')\
                    .agg(sum('sales_count').alias('sales_count'), round(sum('total_sales_amount'),2).alias('total_sales_amount'))

                
            result_df_2.write.format('delta').mode('overwrite').save('s3a://data//gold//daily_product_sales//')
             
        elif silver_orders == product_sales:
            print('Daily product sales is up to date !!!')
        else:
            print('Conflict in versions solve that first !!!')
        
if __name__ == "__main__":
    daily_product_sales(spark)
        