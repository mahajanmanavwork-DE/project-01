from pyspark.sql import SparkSession
from ingestors.load_user_data import load_user_data
from ingestors.load_product_data import load_product_data
from ingestors.load_orders import load_order_data
from ingestors.load_order_items import load_order_items
from ingestors.load_order_status_logs import load_order_status_log
from ingestors.load_inventory_data import load_inventory
import os 
import dotenv
import sys

dotenv.load_dotenv('/opt/spark/env/.env')

spark = SparkSession.builder \
    .appName("BronzeLayerInbound") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ROOT_USER')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_ROOT_PASSWORD')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

if "--date" in sys.argv:
    date = sys.argv[sys.argv.index("--date") + 1]
    req_date = str(date)[:10]
else:
    print('Got no date')
    req_date = '2026-07-02'
    
load_user_data(spark,req_date)
load_product_data(spark,req_date)
load_order_data(spark,req_date)
load_order_items(spark,req_date)
load_order_status_log(spark,req_date)
load_inventory(spark,req_date)


