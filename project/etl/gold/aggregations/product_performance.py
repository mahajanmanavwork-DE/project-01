from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, lag,sum,round
from pyspark.sql.window import Window
import sys 
sys.path.append('/opt/spark')
import os 
import dotenv 
import json
from etl.gold.aggregations.daily_product_sales import daily_product_sales

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

def product_performance(spark):
    
    with open('/opt/spark/etl/versions.json','r') as f:
        data = json.load(f)
        
        daily_product_sales_version = data['schema']['gold']['daily_product_sales']
        product_performance_version = data['schema']['gold']['product_performance']
    
    daily_product_sales(spark)
    print('Daily product sales is now up to date')
    
    if daily_product_sales_version == product_performance_version:
        print('Product performance table is up tp date ')
    elif product_performance_version > daily_product_sales_version:
        print('Version conflict occured, product performance is ahead of daily sales version')
    else:
        daily_product_sales_df = spark.read.format('delta').load('s3a://data//gold//daily_product_sales//')
        daily_product_sales_df.cache()
        prev_day_sales_frame = Window.partitionBy('pid').orderBy('order_date')
        overall_sales = Window.partitionBy('pid').orderBy('order_date').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
        last_7_days_frame = Window.partitionBy('pid').orderBy('order_date').rowsBetween(-6,0)
        rolling_total_frame = Window.partitionBy('pid').orderBy('order_date').rowsBetween(Window.unboundedPreceding,0)
        
        daily_product_sales_df.withColumn('prev_day_sales',lag('total_sales_amount',1,0).over(prev_day_sales_frame))\
            .withColumn('prev_day_order_cnt',lag('sales_count',1,0).over(prev_day_sales_frame))\
            .withColumn('last_7_day_sales',round(sum('total_sales_amount').over(last_7_days_frame),2))\
            .withColumn('overall_sales',round(sum('total_sales_amount').over(overall_sales),2))\
            .withColumn('rolling_total',round(sum('total_sales_amount').over(rolling_total_frame),2))\
            .write.format('delta').mode('overwrite').save('s3a://data//gold//product_performance')
            
        daily_product_sales_df.unpersist()
        
        with open('/opt/spark/etl/versions.json','r+') as f:
            data = json.load(f)
            
            data['schema']['gold']['product_performance'] = data['schema']['gold']['daily_product_sales']
            f.seek(0)
            json.dump(data,f,indent=4)
            f.truncate()
        print('Data written success')

if __name__ == "__main__":
    product_performance(spark)
    

    
    
        