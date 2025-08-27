import org.apache.spark.SparkContext
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.{hour, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties
import spark.implicits._ // 导入隐式转换，简化DataFrame操作

import java.util.Properties

import org.apache.log4j.Logger 
import org.apache.log4j.Level 
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
        (6, 6, 6, 6, 6)
      })
      .toDF("label", "id", "time", "readers", "text")
// 配置JDBC连接属性，用于将处理结果写入MySQL数据库
val pro = new Properties()
pro.setProperty("user", "root")
pro.setProperty("password", "root")

import org.apache.spark.sql.streaming.Trigger

// 将数据写入数据库，并启动流式处理任务
dataTransfrom
    .writeStream.foreachBatch((x: Dataset[Row], y: Long) => {
    x.coalesce(1) // 优化写入操作，合并为单个分区
    x.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://cd81a6d6d30a:5432/postgres", "spark_data", pro) // 写入MySQL
    })
    .trigger(Trigger.ProcessingTime("15 seconds")) // 触发器，每分钟触发一次
    .start()
    .awaitTermination() // 等待处理任务结束

