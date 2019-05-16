package com.data.spark.stream

import java.util.Properties

import com.data.utils.ConfigManager
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import org.slf4j.LoggerFactory;

object StreamIngDemo {

  val LOG = LoggerFactory.getLogger(StreamIngDemo.getClass)

  def main(args: Array[String]): Unit = {

    if(args.length != 1) {
      println("Usage: <properties>")
      LOG.error("properties file not exists")
      System.exit(1)

    }

    //init spark
    val configManager = new ConfigManager(args(0))
    val sparkConf = new SparkConf().setAppName(configManager.getProperty("streaming.appName")).setMaster("local")
    val ssc  = new StreamingContext(sparkConf, Seconds(configManager.getProperty("streaming.interval").toInt))

    //kafkaConsumerParams
    val kafkaConsumerParams = Map[String,  Object] (
      "bootstrap.servers" -> configManager.getProperty("bootstrap.servers")
      ,
      "key.deserializer" -> classOf[StringDeserializer]
      ,
      "value.deserializer" -> classOf[StringDeserializer]
      ,
      "group.id" -> configManager.getProperty("group.id")
      ,
      "auto.offset.reset" -> configManager.getProperty("auto.offset.reset")
      ,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //kafkaProducerParams
    val props = new Properties()
    props.setProperty("metadata.broker.list",configManager.getProperty("metadata.broker.list"))
    props.setProperty("bootstrap.servers",configManager.getProperty("bootstrap.servers"))
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    val inputTopics = Array(configManager.getProperty("input.topics"))
    val outputTopics = configManager.getProperty("output.topics")

    //create stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](inputTopics, kafkaConsumerParams)
    )

    //steam process
    stream.foreachRDD( rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    })





  }

}
