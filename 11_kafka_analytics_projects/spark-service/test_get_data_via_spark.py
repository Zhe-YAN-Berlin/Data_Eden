import os
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Spark-Notebook") \
    .getOrCreate()

df_kafka_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.23.0.5:9092,broker:29092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "checkpoint") \
    .load()

df_kafka_raw.printSchema()
df_kafka_encoded = df_kafka_raw.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
print(type(df_kafka_encoded))
df_kafka_encoded.printSchema()

import pyspark.sql.types as T
import pyspark.sql.functions as F
col = F.split(df_kafka_encoded['value'], ', ')
print(type(df_kafka_encoded['value']))
print(type(col))
print(type(col[0][0][0]))

def parse_info_from_kafka_message(df_raw, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df_raw.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')
    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])

raw_schema = T.StructType(
    [T.StructField("id", T.StringType()),
     T.StructField('time', T.StringType()),
     T.StructField('readers', T.StringType()),
     T.StructField("text", T.StringType())
     ])

df_msg = parse_info_from_kafka_message(df_raw=df_kafka_raw, schema=raw_schema)
df_msg.printSchema()
print(type(df_msg))

df_msg = df_msg.withColumn("id", F.regexp_replace("id", '"id": ', ''))
df_msg = df_msg.withColumn("time", F.regexp_replace("time", '"time": ', ''))
df_msg = df_msg.withColumn("readers", F.regexp_replace("readers", '"readers": ', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", '"text":', ''))


from pyspark.sql.functions import broadcast, col, regexp_replace, expr
names_df = spark.read.text("entities.txt").rdd.map(lambda x: x[0]).collect()
#print(names_df)
broadcast_names = spark.sparkContext.broadcast(names_df)
print(type(broadcast_names))

from pyspark.sql.functions import col

from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

# filtered_df = df_msg.filter(col("text").isin(broadcast_names.value))
# 这个不行 因为text中的字段都是文章 而广播变量是一个个人名

def filter_names(text):
    names = broadcast_names.value
    for name in names:
        if name in text:
            return True
    return False

filter_names_udf = udf(filter_names, BooleanType())
filtered_df = df_msg.filter(filter_names_udf(col("text")))

def sink_console(df, output_mode: str = 'complete', processing_time: str = '30 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .start()
    return write_query # pyspark.sql.streaming.StreamingQuery
write_query = sink_console(df_msg, output_mode='append')