package com.asiainfo.ocsp.shanghai

import com.asiainfo.ocdp.stream.common.Logging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import com.asiainfo.ocsp.shanghai.common._
import com.asiainfo.ocsp.shanghai.util._
import org.apache.commons.lang.{StringEscapeUtils, StringUtils}

/**
  * Created by lidong on 7/8/2018.
  */
object StreamApp extends Logging {


  def main(args: Array[String]) {

    if (args.length < 1) {
      // System.exit(1)
    }

    logInfo("------Stream work dir: " + System.getProperty("user.dir"))
    logInfo("------1.初始化 streamingContext...------")
    val appConf = new AppConf()
    //配置信息
    val sparkConf = new SparkConf()
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
    sparkConf.setAppName("OCSP_KAFKA_ROW_FORMAT")
    val sc = new SparkContext(sparkConf)

    logInfo("------2.启动 streamingContext...------")
    val ssc = new StreamingContext(sc, Seconds(appConf.batch_interval))

    try {
      logInfo("------3.读取topic生成数据流DStream...------")
      val inputStream = readSource(ssc, appConf.broker_list, appConf.topic_name_in, appConf.group_id, appConf.kafka_offset_type, appConf.kafka_kerberos_enable, appConf.kafka_location_preferbroker)
      logInfo("3.1 读取topic生成DStream完毕...")

      inputStream.foreachRDD(rdd => {
        logInfo("------4.start inputStream流数据处理...------")
        val begin = System.currentTimeMillis()

        val kvRDD = rdd.map(r => {
          val raw = r.value()
          // kafka输入topic中数据，如：46000**,20180101141250,...
          val inputArr = StringUtils.splitByWholeSeparatorPreserveAllTokens(raw, StringEscapeUtils.unescapeJava(appConf.default_separator)) //可以识别任意的分隔符空白

          var key_str = ""
          for (i <- 0 until appConf.key_index.length) {
            if (i == appConf.key_index.length - 1) {
              key_str += inputArr(appConf.key_index(i).toInt)
            } else {
              key_str += inputArr(appConf.key_index(i).toInt) + "_"
            }
          }
          (key_str, inputArr)
        })

        //返回去重后的rdd
        val distinctRDD = kvRDD.reduceByKey((inputArr1: Array[String], inputArr2: Array[String]) => {
          reduceByKeyOperate(inputArr1, inputArr2, appConf.time_index, appConf.distinct_time_enable)
        }).persist()

        //输出去重后的rdd到kafka
        distinctRDD.foreachPartition(partition => {
          partition.foreach(map => {
            KafkaSendTool.sendMessage(new ProducerRecord[String, String](appConf.topic_name_out, map._1, map._2.toString), appConf.broker_list, appConf.kafka_kerberos_enable)
          })
        })

        distinctRDD.unpersist()
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
    * reduceByKey时取最新信令时间的数据
    * @param inputArr1
    * @param inputArr2
    * @param timeIndex
    * @param isDistinctByTime
    * @return
    */
  private def reduceByKeyOperate(inputArr1: Array[String], inputArr2: Array[String], timeIndex: Int, isDistinctByTime: Boolean) = {
    if (isDistinctByTime) {
      val res = if (inputArr1(timeIndex) > inputArr2(timeIndex)) inputArr1 else inputArr2
      res
    } else {
      val res = inputArr1 //随机取
      res
    }
  }

  /**
    * 读取kafka源数据
    *
    * @param ssc
    * @return
    */
  def readSource(ssc: StreamingContext, broker_list: String, topic_name: String, group_id: String, offset_reset: String, kafka_kerberos_enable: Boolean, kafka_location_preferbroker: Boolean): DStream[ConsumerRecord[String, String]] = {

    //根据输入数据接口配置，生成数据流 DStream
    val dataSource = new KafkaReader(ssc, kafka_kerberos_enable, kafka_location_preferbroker, broker_list, topic_name, group_id, offset_reset)
    dataSource.createStreamMulData()
  }

}
