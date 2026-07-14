from pyspark.sql import SparkSession 
from pyspark.sql.functions import col,sum,count,avg,ntile,desc,when,datediff,max,lit,current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os
import dotenv
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


# df = spark.read.format('delta').load('s3a://data//gold//customer_360//')
# df.orderBy(desc('total_order_value')).show(100)

# df = spark.read.format('delta').load('s3a://data//gold//daily_product_sales//')
# df.show(100)

# df = spark.read.format('delta').load('s3a://data//gold//daily_product_sales//')
# df.orderBy('pid','order_date').show(100)

# df = spark.read.format('delta').load('s3a://data//gold//product_performance//')
# df.orderBy('pid','order_date').show(100)

df = spark.read.format('delta').load('s3a://data//gold//category_trends//')
df.show(100)

# orders_path = 's3a://data//silver//orders//'
# orders_df = spark.read.format('delta').option('readChangefeed','true').option('startingVersion',0).load(orders_path)
# orders_df.show(1000)
 
 
# ------------------------------------------------------------------------------------------
# product_sales = 0
# product_df = spark.read.format('delta').load('s3a://data//silver//products//')
# orders_df = spark.read.format('delta').option('readChangefeed',True).option('startingVersion',product_sales).option('endingversion',product_sales).load('s3a://data//silver//orders//')
# order_items_df = spark.read.format('delta').option('readChangefeed',True).option('startingVersion',product_sales).option('endingversion',product_sales).load('s3a://data//silver//order_items//')

# product_df.show()
# print('product_df')
# orders_df.show()
# print('orders_df')
# order_items_df.show()
# print('order_items_df')

# joined_df = orders_df.join(order_items_df,orders_df.oid == order_items_df.order_id, 'inner')
# joined_df.show()
# print('joined_df')

# df2 = joined_df.join(product_df, col("product_id") == col("pid"),'inner')\
#         .select(product_df.pid,product_df.product_name,product_df.category,product_df.sub_category,product_df.brand,product_df.base_price,joined_df.order_date, joined_df.quantity, joined_df.unit_price_at_order)
# df2.show()
# print('df2')
                        
# result_df = df2.groupBy('pid','product_name','category','sub_category','brand','base_price','order_date')\
#         .agg(sum('quantity').alias('sales_count'), sum(col('quantity') * col('unit_price_at_order') ).alias('total_sales_amount'))
# result_df.show()
# print('result_df')
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
# bronze_prod = spark.read.format('delta').load('s3a://data//bronze//products')
# bronze_prod.show()
# print('bronze_prod')

# silver_prod = spark.read.format('delta').load('s3a://data//silver//products')
# silver_prod.show()
# print('silver_prod')
# ------------------------------------------------------------------------------------------

                
    