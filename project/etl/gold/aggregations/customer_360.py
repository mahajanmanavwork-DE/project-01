import sys 
sys.path.append('/opt/spark/etl/')
# print(sys.path)
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col,sum,count,ntile,desc,when,datediff,max,lit,current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable
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

def customer_360(spark):
    try:
        with open('/opt/spark/etl/versions.json','r') as f:
            data = json.load(f)
            bronze_customers = data['schema']['bronze']['user_data']
            silver_customers = data['schema']['silver']['user_data']
            
            bronze_orders = data['schema']['bronze']['orders']
            silver_orders = data['schema']['silver']['orders']
            customers_360 = data['schema']['gold']['customer_360']
            
        if silver_orders == customers_360:
            
            print(f'Gold layer is up to date as of version {customers_360}')
        elif bronze_customers < silver_customers or bronze_orders < silver_orders:
            
            print('Error conflict in versions as one version can never be ahead of its dependents versions')
        else:
            if bronze_customers > silver_customers:
                cleaned_users(spark)
                    
            if bronze_orders > silver_orders:
                cleaned_orders(spark)
            with open('/opt/spark/etl/versions.json','r') as f:
                data = json.load(f)
                # bronze_customers = data['schema']['bronze']['user_data']
                # silver_customers = data['schema']['silver']['user_data']
            
                # bronze_orders = data['schema']['bronze']['orders']
                silver_orders = data['schema']['silver']['orders']
                customers_360 = data['schema']['gold']['customer_360']
                                
            for _ in range(silver_orders - customers_360):
                customers_360 +=1 
                orders_data = spark.read.format('delta').option('readChangeFeed',True).option('startingVersion', customers_360).load('s3a://data//silver//orders//')
                
                # oid, customer_id, order_date, order_status, payment_method, shipping_address, total_amount, discount_applied, final_amount
                orders_df = orders_data.drop("_change_type","_commit_version","_commit_timestamp")
                
                # customer_id
                customer_ids = orders_df.select(col('customer_id')).distinct()
                
                # oid, customer_id, order_date ,order_status ,payment_method ,shipping_address ,total_amount ,discount_applied ,final_amount 
                all_silver_orders = spark.read.format('delta').load('s3a://data//silver//orders')
                
                # oid, customer_id, order_date, order_status, payment_method, shipping_address, total_amount, discount_applied, final_amount
                req_silver_orders = all_silver_orders.join(customer_ids, all_silver_orders.customer_id == customer_ids.customer_id, 'left_semi')
                
                # customer_id, total_order_value, total_orders, last_order_date, customer_tier, churn_risk_score, updated_at
                df = req_silver_orders.groupBy('customer_id').agg(sum("final_amount").alias('total_order_value')\
                    ,count('oid').alias('total_orders')\
                    ,max('order_date').alias('last_order_date') )\
                    .withColumn('customer_tier',lit('None'))\
                    .withColumn('churn_risk_score',lit('None'))\
                    .withColumn('updated_at',current_timestamp())
                
                # customer_id, name, email, city, country, join_date
                customer_df = spark.read.format('delta').load('s3a://data//silver//user_data//')
                
                # customer_id, name, email, city, country, join_date, total_order_value, total_orders, customer_tier, last_order_date, churn_risk_score, updated_at
                main_df = df.join(customer_df, df.customer_id == customer_df.customer_id, 'left')\
                    .select(customer_df.customer_id,customer_df.name,customer_df.email,customer_df.city,customer_df.country,customer_df.join_date,\
                        df.total_order_value,df.total_orders,df.customer_tier,df.last_order_date,df.churn_risk_score,df.updated_at).dropDuplicates()
                try:
                    gold_customer_360 = DeltaTable.forPath(spark,'s3a://data//gold//customer_360//')
                    
                    gold_customer_360.alias('g').merge(
                        main_df.alias('m'),
                        "g.customer_id = m.customer_id "
                    ).whenMatchedUpdate(
                        set= {
                            "total_order_value":"m.total_order_value",
                            "total_orders":"m.total_orders",
                            "customer_tier":"m.customer_tier",
                            "last_order_date":"m.last_order_date",
                            "churn_risk_score":"m.churn_risk_score",
                            "updated_at":current_timestamp()
                        }
                    ).whenNotMatchedInsert(
                        values = {
                            "customer_id":"m.customer_id",
                            "name": "m.name",
                            "email":"m.email",
                            "city":"m.city",
                            "country":"m.country",
                            "join_date":"m.join_date",
                            "total_order_value":"m.total_order_value",
                            "total_orders":"m.total_orders",
                            "customer_tier":"m.customer_tier",
                            "last_order_date":"m.last_order_date",
                            "churn_risk_score":"m.churn_risk_score",
                            "updated_at":current_timestamp()}
                        ).execute()
                    
                except Exception as e:
                    print(f'Exception is {e}')
                    main_df.dropDuplicates().write.format('delta').mode('append').save('s3a://data//gold//customer_360//')

                gold_df = spark.read.format('delta').load('s3a://data//gold//customer_360//')
                gold_table = DeltaTable.forPath(spark,'s3a://data//gold//customer_360//')
                
                # customer_id, customer_tier
                frame = Window.orderBy(desc('total_order_value'))
                df1 = gold_df.withColumn('ntile',ntile(4).over(frame))\
                    .withColumn('customer_tier',when(col('ntile') == 1, 'Platinum' )\
                        .when(col('ntile') == 2, 'Gold' )\
                        .when(col('ntile') == 3, 'Silver' )\
                        .otherwise('bronze'))\
                        .select("customer_id","customer_tier")
                
                # customer_id, churn_risk_score
                df2 = gold_df.withColumn('date_diff',datediff('updated_at','last_order_date'))\
                    .withColumn('churn_risk_score', when(col('date_diff') > 30, 'High')\
                    .when(col('date_diff') > 15, 'Medium')\
                    .otherwise('Low'))\
                    .select('customer_id',"churn_risk_score")
                
                merge_df = df1.join(df2, df1.customer_id == df2.customer_id,'inner')\
                            .select(df1.customer_id, df1.customer_tier, df2.churn_risk_score)
                
                gold_table.alias('g').merge(
                    merge_df.alias('m'),
                    "g.customer_id = m.customer_id"
                ).whenMatchedUpdate(
                    set = {
                        "customer_tier":"m.customer_tier",
                        "churn_risk_score":"m.churn_risk_score"
                    }
                ).execute()
                    
                

                with open('/opt/spark/etl/versions.json','r+') as f:
                    data = json.load(f)
                    data['schema']['gold']['customer_360'] +=1
                    f.seek(0)
                    json.dump(data,f,indent=4)
                    f.truncate()
    except Exception as e:
        print(f'Error is {e}')

if __name__ == "__main__":
    customer_360(spark)