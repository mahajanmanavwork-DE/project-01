import sys 
sys.path.append('/opt/spark')
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,max,round,avg,col,dense_rank,desc,count_distinct
from pyspark.sql.window import Window
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

def category_trends(spark):
    with open('/opt/spark/etl/versions.json','r') as f:
        data = json.load(f)
        
        daily_product_sales_version = data['schema']['gold']['daily_product_sales']
        category_trends_version = data['schema']['gold']['category_trends']
        
    if daily_product_sales_version == category_trends_version:
        print('Category trends table is upto date ')
    elif daily_product_sales_version < category_trends_version:
        print('Conflict in version !!!!!!!')
    else:
        daily_product_sales_df = spark.read.format('delta').load('s3a://data//gold//daily_product_sales//')
            
        df1 = daily_product_sales_df.groupBy('category')\
            .agg(max('order_date').alias('date')\
                ,round(avg(col('total_sales_amount')),2).alias('daily_revenue')\
                ,round(avg(col('sales_count')),0).alias('daily_orders'))
            
        orders_df = spark.read.format('delta').load('s3a://data//silver//orders//')
        order_items_df = spark.read.format('delta').load('s3a://data//silver//order_items//')
        products_df = spark.read.format('delta').load('s3a://data//silver//products//')
            
        merged_df = orders_df.join(order_items_df, orders_df.oid == order_items_df.order_id,'inner')\
            .join(products_df,order_items_df.product_id == products_df.pid,'inner')\
            .select(orders_df.oid,orders_df.customer_id, order_items_df.product_id, order_items_df.line_total,products_df.category,products_df.product_name)
            
        merged_df.cache()
            
        frame = Window.partitionBy('category').orderBy(desc('top_product_revenue'))
        df2 = merged_df.groupBy('category',"product_name")\
            .agg(round(sum('line_total'),2).alias('top_product_revenue'))\
            .withColumn('rank',dense_rank().over(frame))\
            .filter(col('rank') == 1).drop('rank')
            
        df3 = merged_df.groupBy('category')\
            .agg(count_distinct('customer_id').alias('unique_customers'))
                
        agg_df = df2.join(df3,df2.category == df3.category, 'inner')\
            .select(df2.category,df2.product_name, df2.top_product_revenue, df3.unique_customers)
            
        df1.join(agg_df, df1.category == agg_df.category,'inner')\
            .select(df1.category, df1.date, df1.daily_revenue, df1.daily_orders, agg_df.unique_customers, col("product_name").alias('top_product'), agg_df.top_product_revenue  )\
            .write.format('delta').mode('overwrite').save('s3a://data//gold//category_trends')
            
        merged_df.unpersist()
                
                
                
        with open('/opt/spark/etl/versions.json','r+') as f:
            data = json.load(f)
            
            data['schema']['gold']['category_trends'] = data['schema']['gold']['daily_product_sales']
            f.seek(0)
            json.dump(data,f,indent=4)
            f.truncate()
            
        print('Done ')
    
if __name__ == "__main__":
    category_trends(spark)
                
        
