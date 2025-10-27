from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--exec_date", required=True)
args = parser.parse_args()

execution_date = args.execution_date

spark = SparkSession.builder \
        .appName("Load HDFS to Hive") \
        .enableHiveSupport() \
        .getOrCrereate()

hdfs_path = f"hdfs:///path/to/data/{execution_date}"

df = spark.read.parquet(hdfs_path)

df_clean=df.dropna().dropDuplicates()
df_clean.write.mode("Append").partitonBy("date").saveAsView("raw_data_view")

spark.stop()

