# /opt/jobs/spark_submit_job.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import functions as F

def parse_args(argv):
    return input_base, output_base, hive_db, hive_table, party_key, part_val

def main(argv):
    input_base, output_base, hive_db, hive_table, party_key, part_val = parse_args(argv)
    spark = (
        SparkSession.builder
        .appNanme("HDFS to HIVE")
        .enableHiveSupport()
        .config("spark.sql.spirces.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    input_path = f"hdfs://{path}"
    df = spark.read.json(input_path)


    result = (df.groupBy("key")
              .agg((F.sum("amount").alias("total_amount")))
              )
    
    result.write.mode("overwrite").parquet(f"{output_base}/party_key={part_val}/")
    result.write.mode("append").partitionBy(party_key).format("hive").saveAsTable(f"{hive_db}.{hive_table}")
    
    spark.stop()

if __name__ == "__main__":
    main(sys.argv)