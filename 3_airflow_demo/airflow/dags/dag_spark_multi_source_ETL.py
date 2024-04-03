import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import gdown
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import col, acos, cos, sin, radians, atan2, sqrt
from pyspark.sql.window import Window
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

credentials_location = '/home/datatalks_jan/.google/credentials/google_credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('conrad_test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

def download_and_unzip_data():
    source_url = 'https://drive.google.com/uc?id=1vcb_HBWsOSKW4XxhLfRpGlLzBLwHlGWJ'
    output_path = 'source_data/multi_source_data'
    output_file = os.path.join(output_path, 'multi_source_demo.zip') 
    gdown.download(source_url, output_file, quiet=False)
    os.system(f'unzip -o {output_file} -d {output_path}')

def spark_etl_data():
    output_path = 'source_data/multi_source_data'
    df_1_users = spark.read.option("header", "true").csv(f'{output_path}/users.csv')
    df_1_distri_centers = spark.read.option("header", "true").csv(f'{output_path}/distribution_centers.csv')

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


    df_2_orders = spark.read.option("header", "true").csv(f'{output_path}/orders.csv')
    df_2_order_items = spark.read.option("header", "true").csv(f'{output_path}/order_items.csv')

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

    df_3_product_price = spark.read.option("header", "true").csv(f'{output_path}/products.csv')
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
def data_outbound_GCS():
    df_final.write.csv("gs://my-zhe-414813/conrad_output", mode="overwrite", header=True, inferSchema=True)

default_args = {
    'owner': 'zhe', #key!
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('conrad_coding', default_args=default_args, schedule_interval='@weekly') as dag:
    task_1_data_ingestion = PythonOperator(
        task_id='task_1_data_ingestion',
        python_callable=download_and_unzip_data
    )

    task_2_spark = PythonOperator(
        task_id='task_2_spark',
        python_callable=spark_etl_data
    )

    task_3_data_output = PythonOperator(
        task_id='task_3_data_output',
        python_callable=data_outbound_GCS
    )

    task_1_data_ingestion >> task_2_spark >> task_3_data_output