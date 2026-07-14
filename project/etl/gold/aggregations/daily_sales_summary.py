import sys 
sys.path.append('/opt/spark/etl/')
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col,sum,count,avg,ntile,desc,when,datediff,max,lit,current_timestamp # type: ignore
from pyspark.sql.window import Window # type: ignore
from pyspark.sql.types import DateType # type: ignore
from delta.tables import DeltaTable # type: ignore
from silver.data_enrichment.users import cleaned_users
from silver.data_enrichment.orders import cleaned_orders

import os 
import dotenv
import json
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

def sales_summary(spark):
    with open('/opt/spark/etl/versions.json','r') as f:
        data = json.load(f)
        
        silver_orders = data['schema']['silver']['orders']
        daily_sales_summary = data['schema']['gold']['daily_sales_summary']
        
        if silver_orders == daily_sales_summary:
            print('Sales summary is up to date')
        elif daily_sales_summary > silver_orders:
            print('Conflict in versions fix that ')
        else:
            for _ in range(silver_orders - daily_sales_summary):
                orders_path = 's3a://data//silver//orders//'
                daily_sales_summary +=1
                print(f'Reading daily sales suummary for version {daily_sales_summary}')
                orders_df = spark.read.format('delta').option('readChangefeed','true').option('startingVersion',daily_sales_summary).load(orders_path)
                
                merged_df = orders_df.groupBy(col('order_date').cast(DateType())).agg(
                    count('oid').alias('order_count'),
                    sum('total_amount').alias('total_amount'),
                    avg('discount_applied').alias('avg_discount_per_order'),
                    sum('final_amount').alias('net_revenue')
                )
                try:
                    daily_sales_summary_df = spark.read.format('delta').load('s3a://data//gold//daily_sales_summary//')
                    df = daily_sales_summary_df.union(merged_df)
                    
                    merged_df_2 = df.groupBy(col('order_date').cast(DateType())).agg(
                        sum('order_count').alias('order_count'),
                        sum('total_amount').alias('total_amount'),
                        avg('avg_discount_per_order').alias('avg_discount_per_order'),
                        sum('net_revenue').alias('net_revenue')
                    )
                except:
                    merged_df_2 = merged_df
                
                merged_df_2.write.format('delta').mode('overwrite').save('s3a://data//gold//daily_sales_summary//')
                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    
                    data['schema']['gold']['daily_sales_summary'] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
                
if __name__ == "__main__":
    sales_summary(spark)
                    
                    
                
                
                    

                
                
                
