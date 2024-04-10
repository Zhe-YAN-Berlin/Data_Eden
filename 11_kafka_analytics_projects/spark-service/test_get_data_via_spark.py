import os
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast, regexp_replace, expr, udf, split
from pyspark.sql.functions import col,StringType

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

'''df_kafka_raw.printSchema()
df_kafka_encoded = df_kafka_raw.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
print(type(df_kafka_encoded))
df_kafka_encoded.printSchema()

col = F.split(df_kafka_encoded['value'], ', ')
print(type(df_kafka_encoded['value']))
print(type(col))'''

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

new_schema = T.StructType(
    [T.StructField("label", T.StringType()),
     T.StructField("id", T.StringType()),
     T.StructField('time', T.StringType()),
     T.StructField('readers', T.StringType()),
     T.StructField("text", T.StringType())
     ])

df_msg = parse_info_from_kafka_message(df_raw=df_kafka_raw, schema=new_schema)
df_msg.printSchema()
print(type(df_msg))

df_msg = df_msg.withColumn("label", F.regexp_replace("label", '"', ''))
df_msg = df_msg.withColumn("label", F.regexp_replace("label", 'label: ', ''))
df_msg = df_msg.withColumn("label", F.regexp_replace("label", '\\{', ''))
df_msg = df_msg.withColumn("id", F.regexp_replace("id", '"id": ', ''))
df_msg = df_msg.withColumn("id", F.regexp_replace("id", '"', ''))
df_msg = df_msg.withColumn("id", F.regexp_replace("id", '\\{', ''))
df_msg = df_msg.withColumn("time", F.regexp_replace("time", '"time": ', ''))
df_msg = df_msg.withColumn("readers", F.regexp_replace("readers", '"readers": ', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", '"text":', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", '\\}', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", '\\.', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", ':', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", '"', ''))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", "'", " "))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", "\\*", ""))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", "`", ""))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", "\\?", ""))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", "#", ""))
df_msg = df_msg.withColumn("text", F.regexp_replace("text", "!", ""))

names_df = spark.read.text("entities.txt").rdd.flatMap(lambda x: x).collect()
broadcast_names = spark.sparkContext.broadcast(names_df)

#names_df = ["Alice","Jan","Zoe"]
#broadcast_names = spark.sparkContext.broadcast(names_df)

'''from pyspark.sql.functions import split
def filter_text(text):
    return text
    #filtered_words = [value for value in broadcast_names.value if value in text]
    #return ' '.join(filtered_words)
filter_text_udf = udf(filter_text, StringType())'''

from pyspark.sql.functions import col, split, explode, array_intersect, array_join, lit, array
split_df = df_msg.withColumn("split_text", split(col("text"), " "))
filtered_names_df = split_df.withColumn("filtered_names", array_intersect(col("split_text"), array(*[lit(name) for name in broadcast_names.value])))

def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .start()
    return write_query # pyspark.sql.streaming.StreamingQuery

write_query = sink_console(filtered_names_df, output_mode='append')