from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
spark = SparkSession.builder \
    .appName("silverLayerInbound") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
    

# df = spark.read.format('delta').load('s3a://data//silver//user_data')
# print(f'count for user {df.count()}')

# df = spark.read.format('delta').load('s3a://data//silver//products')
# print(f'count for products {df.count()}')

# df = spark.read.format('delta').load('s3a://data//silver//orders')
# df.show()
# print(f'count for orders {df.count()}')

df = spark.read.format('delta').load('s3a://data//silver//order_items')
df.show()
# print(f'count for order_items {df.count()}')

# df = spark.read.format('delta').load('s3a://data//silver//order_status_logs')
# print(f'count for order_status_logs {df.count()}')

# df = spark.read.format('delta').load('s3a://data//silver//inventory')
# print(f'count for inventory {df.count()}')
