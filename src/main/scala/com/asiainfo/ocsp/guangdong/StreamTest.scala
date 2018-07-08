package com.asiainfo.ocsp.guangdong

import com.asiainfo.ocdp.stream.common.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Created by lidong on 11/28/2017.
  */
object StreamTest extends Logging {

  def main(args: Array[String]) {

    if (args.length != 1) {
      logInfo("------请传入参数...------")
      System.exit(1)
    }

    val batch_interval = 30
    val broker_list = "192.250.1.250:21007,192.250.2.68:21007"
    val topic_name = args(0)
    val group_id = "default_group_guangdong"
    val auto_offset_reset = "latest"

    logInfo("------Stream work dir: " + System.getProperty("user.dir"))
    logInfo("------1.初始化 streamingContext...------")

    val sparkConf = new SparkConf()
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
    sparkConf.setAppName("OCSP_KAFKA_TEST")
    val sc = new SparkContext(sparkConf)

    logInfo("------2.启动 streamingContext...------")
    val ssc = new StreamingContext(sc, Seconds(batch_interval))

    try {
      logInfo("------3.读取topic生成数据流DStream...------")
      val inputStream = readSource(ssc, broker_list, topic_name, group_id, auto_offset_reset)

      logInfo("------4. start processing DStream...------")
      // inputStream.print()
      // inputStream.saveAsTextFiles("file:///home/ocdc/OCSP/conf/output.txt")

      inputStream.foreachRDD(rdd => {
        logInfo("------4.1 循环DStream数据处理...------")
        val begin = System.currentTimeMillis()

        rdd.foreach(
          record => {
            logInfo("------print message------key:" + record.key() + "value:" + record.value())
          }
        )

        val end = System.currentTimeMillis()
        logInfo("------5.All datas processed cost time : " + (end - begin) / 1000 + "s ! ")
      })

      ssc.start()
      ssc.awaitTermination()
    } catch {
      case err: Error => {
        err.printStackTrace()
        logError("------stream task goes wrong!------" + err.getStackTrace)
      }
    } finally {
      System.exit(0)
    }
  }


  /**
    * 读取kafka源数据
    *
    * @param ssc
    * @return
    */
  def readSource(ssc: StreamingContext, broker_list: String, topic_name: String, group_id: String, offset_reset: String): DStream[ConsumerRecord[String, String]] = {

    val mKafkaParams = Map[String, Object](ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offset_reset
      , ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      , ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      , ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
      , CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> "SASL_PLAINTEXT"
      , CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> broker_list
      , "sasl.kerberos.service.name" -> "kafka"
      , ConsumerConfig.GROUP_ID_CONFIG -> group_id)

    val mTopicsSet = {
      topic_name.split(",").toSet
    }

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](mTopicsSet, mKafkaParams))

  }

}
