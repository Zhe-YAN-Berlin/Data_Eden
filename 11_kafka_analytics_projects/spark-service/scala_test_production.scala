import org.apache.spark.SparkContext
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.{col, explode, window}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.util.Properties
import spark.implicits._ // 导入隐式转换，简化DataFrame操作

import java.util.Properties

import org.apache.log4j.Logger 
import org.apache.log4j.Level 

import org.apache.spark.sql.functions.{col, split, array_intersect, array_join, lit, array, explode}
import org.apache.spark.sql.functions.expr

Logger.getLogger("org").setLevel(Level.WARN) 
Logger.getLogger("akka").setLevel(Level.WARN)
sc.setLogLevel("ERROR") 

// 从Kafka读取数据流
val dataStream: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "680ceacd1a9a:9092,broker:29092") // Kafka服务器配置
    .option("subscribe", "events") // 订阅的Kafka主题
    .load()

// 对数据进行转换处理
val dataTransfrom = dataStream.selectExpr("CAST(value AS STRING)").as[String]
      .map(item => {
        val arrays: Array[String] = item.split(",\\s+\"")
        (arrays(0), arrays(1), arrays(2), arrays(3), arrays(4))
      })
      .toDF("label", "id", "time_text", "readers", "text")
      .withColumn("label", regexp_replace(regexp_replace(regexp_replace(col("label"), "\"", ""), "label: ", ""), "\\{", ""))
      .withColumn("id", regexp_replace(regexp_replace(regexp_replace(col("id"), "id", ""), "\"", ""), ":", ""))
      .withColumn("time_text", trim(regexp_replace(regexp_replace(col("time_text"), "\"", ""), "time: ", "")))
      .withColumn("time_new_1",from_unixtime($"time_text".cast(LongType),"yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .withColumn("time_new_2",date_format($"time_new_1","yyyy-MM-dd HH:mm:ss"))
      
      .withColumn("readers", regexp_replace(regexp_replace(col("readers"), "\"", ""), "readers: ", ""))
      .withColumn("text", regexp_replace(col("text"), "[\\p{Punct}]", ""))
      .withColumn("readers", col("readers").cast("int"))

      //.withColumn("text", regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(col("text"), "text:", ""), "\\}", ""), "\\.", ""), ":", ""), "\"", ""), "'", " "), "\\*", ""))
      //.withColumn("text", regexp_replace(regexp_replace(col("text"), "`", ""), "\\?", ""))
      //.withColumn("text", regexp_replace(regexp_replace(col("text"), "#", ""), "!", ""))

// 广播变量定义
val broadcast_names = spark.sparkContext.broadcast(spark.read.textFile("entities.txt").collect())

//处理text中的文字
val split_df = dataTransfrom.withColumn("split_text", split(col("text"), " "))


//与广播变量取交集
val target_names_df = split_df.withColumn("target_names", array_intersect(col("split_text"), array(broadcast_names.value.map(lit): _*)))

//拆分名字+计算出现次数
val target_name_count_df = target_names_df
  .select(explode(col("target_names")).as("name"), col("label"), col("id"), col("time_text"),col("time_new_1"),col("time_new_2"), col("readers"))
  .withWatermark("time_new_1", "15 seconds") 
  .groupBy(window(col("time_new_1"), "15 seconds"), col("name"), col("label"), col("id"), col("time_text"),col("time_new_2"), col("readers"))
  .count()

// 配置JDBC连接属性，用于将处理结果写入MySQL数据库
val pro = new Properties()
pro.setProperty("user", "root")
pro.setProperty("password", "root")

import org.apache.spark.sql.streaming.Trigger

// 将数据写入数据库，并启动流式处理任务
target_name_count_df
    .drop("time_new_1", "window")
    .writeStream.foreachBatch((x: Dataset[Row], y: Long) => {
    x.printSchema() // 打印schema类型
    x.coalesce(1) // 优化写入操作，合并为单个分区
    x.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://cd81a6d6d30a:5432/postgres", "spark_data", pro) // 写入MySQL
    })
    .trigger(Trigger.ProcessingTime("15 seconds")) // 触发器，每15s触发一次
    .start()
    .awaitTermination() // 等待处理任务结束

