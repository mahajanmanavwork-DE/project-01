from pyspark.sql import SparkSession 
from data_enrichment.users import cleaned_users
from data_enrichment.products import cleaned_products
from data_enrichment.order_items import cleaned_order_items
from data_enrichment.orders import cleaned_orders
from data_enrichment.inventory import cleaned_inventory
from data_enrichment.order_status_log import cleaned_order_staus_logs

spark = SparkSession.builder \
    .appName("BronzeLayerInbound") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

cleaned_users(spark)
cleaned_products(spark)
cleaned_order_items(spark)
cleaned_orders(spark)
cleaned_inventory(spark)
cleaned_order_staus_logs(spark)


    
