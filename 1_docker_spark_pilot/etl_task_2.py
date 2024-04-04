import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import col, acos, cos, sin, radians, atan2, sqrt
from pyspark.sql.window import Window
from datetime import datetime

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('conrad_test')

sc = SparkContext(conf=conf)

test = []
test0 = []

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df_1_users = spark.read.option("header", "true").csv('/opt/spark/mount_data/users.csv')
df_1_distri_centers = spark.read.option("header", "true").csv('/opt/spark/mount_data/distribution_centers.csv')

df_1_users = df_1_users.withColumn("latitude", df_1_users["latitude"].cast("float"))
df_1_users = df_1_users.withColumn("longitude", df_1_users["longitude"].cast("float"))
df_1_distri_centers = df_1_distri_centers.withColumn("latitude", df_1_distri_centers["latitude"].cast("float"))
df_1_distri_centers = df_1_distri_centers.withColumn("longitude", df_1_distri_centers["longitude"].cast("float"))

def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = 6371 * c 
    return distance

df_1_users.createOrReplaceTempView("users")
df_1_distri_centers.createOrReplaceTempView("distribution_centers")

df_1_sql_query = """
    SELECT u.id as user_id, u.age as user_age, u.country, u.state,
        dc.id AS distribution_center_id,dc.name AS distribution_center_name,
        u.latitude as user_latitude, u.longitude as user_longitude, dc.latitude as center_latitude, dc.longitude as center_longitude
    FROM users u
    CROSS JOIN distribution_centers dc
"""

df_1_with_distance = spark.sql(df_1_sql_query).withColumn("distance", haversine_distance(col("user_latitude"), col("user_longitude"), col("center_latitude"), col("center_longitude")))

windowSpec = Window.partitionBy("user_id").orderBy("distance")
df_with_row_number = df_1_with_distance.withColumn("row_number", row_number().over(windowSpec))
df_1_final = df_with_row_number.filter(col("row_number") == 1)
df_1 = df_1_final.select("user_id","user_age","country","state","distribution_center_id","distribution_center_name","distance").orderBy("user_id")


df_2_orders = spark.read.option("header", "true").csv('/opt/spark/mount_data//orders.csv')
df_2_order_items = spark.read.option("header", "true").csv('/opt/spark/mount_data//order_items.csv')

df_2_orders_last_year = df_2_orders.withColumn("created_at", df_2_orders["created_at"].cast("timestamp")).filter(year("created_at") == 2023)

df_2_orders_last_year.createOrReplaceTempView("orders_last_year")
df_2_order_items.createOrReplaceTempView("order_items")

df_2_sql = spark.sql("""
    SELECT t2.order_id, t1.user_id, t1.product_id, t1.status, t1.sale_price
    FROM  order_items as t1
    INNER JOIN orders_last_year as t2 on t2.order_id = t1.order_id
""")

df_2_sql.createOrReplaceTempView("order_details")
df_2 = spark.sql("""
    SELECT user_id, COUNT(CASE WHEN status = 'Returned' THEN product_id END) / COUNT(product_id) as product_return_rate
    FROM  order_details
    GROUP BY user_id
""")

df_3_product_price = spark.read.option("header", "true").csv('/opt/spark/mount_data/products.csv')
df_3_product_price = df_3_product_price.select("id", "cost")
df_3_product_price.createOrReplaceTempView("product_price")

df_3 = spark.sql("""
    SELECT user_id, SUM(T1.sale_price - T2.cost) as profit_total
    FROM  order_details AS T1
    LEFT JOIN product_price AS T2 ON T2.id = T1.product_id
    GROUP BY user_id
""")

df_final = df_1.join(df_2, "user_id").join(df_3, "user_id")
df_final = df_final.select("user_id","user_age","country","state","distribution_center_id","distribution_center_name","product_return_rate","profit_total")
df_final = df_final.withColumn("user_age", df_final["user_age"].cast("int"))

spark.stop()