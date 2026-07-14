import sys
sys.path.append('/opt/spark')
from pyspark.sql import SparkSession
import os 
import dotenv 
from aggregations.customer_360 import customer_360
from aggregations.daily_sales_summary import sales_summary
from aggregations.daily_product_sales import daily_product_sales
from aggregations.product_performance import product_performance
from aggregations.category_trends import category_trends

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
    
def main(spark):
    try:
        customer_360(spark)
    except Exception as e:
        print(f'Exception in customer 360 as {e}')
    
    try:
        sales_summary(spark)
    except Exception as e:
        print(f'Exception in sales summary as {e}')
    
    try:
        daily_product_sales(spark)
    except Exception as e:
        print(f'Exception in daily product sales as {e}')
    
    try:
        product_performance(spark)
    except Exception as e:
        print(f'Exception in product performance as {e}')
        
    try:
        category_trends(spark)
    except Exception as e:
        print(f'Exception in category trends as {e}')

if __name__ == "__main__":
    main(spark)
    
    