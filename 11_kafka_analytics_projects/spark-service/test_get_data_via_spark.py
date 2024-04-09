import os
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("Spark-Notebook") \
    .getOrCreate()

df_kafka_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "checkpoint") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

df_kafka_raw.printSchema()
df_kafka_encoded = df_kafka_raw.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
df_kafka_encoded.printSchema()

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

info_schema = T.StructType(
    [T.StructField("id", T.IntegerType()),
     T.StructField('time', T.TimestampType()),
     T.StructField('readers', T.IntegerType()),
     T.StructField("text", T.StringType())
     ])

df_msg = parse_info_from_kafka_message(df_raw=df_kafka_raw, schema=info_schema)
df_msg.printSchema()

def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query # pyspark.sql.streaming.StreamingQuery

write_query = sink_console(df_msg, output_mode='append')