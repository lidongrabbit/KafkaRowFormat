package com.asiainfo.ocsp.yunnan

import java.io.FileInputStream
import java.util.Properties

import com.asiainfo.ocdp.stream.common.Logging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable

/**
  * Created by lidong on 10/29/2017.
  */
object StreamApp extends Logging {

  // kafka topic数据列分隔符
  val separator_column_field = ","
  // kafka topic数据行分隔符
  val separator_row_field = ";"

  // 是否开启kerberos
  val kerberos_enable = true
  // LocationStrategies.PreferBrokers OR LocationStrategies.PreferConsistent
  val kafka_location_preferbroker = false
  val kafka_kerberos_enable = true

  // kafka是否开启kerberos
  def main(args: Array[String]) {

    if (args.length < 1) {
      // System.exit(1)
    }

    // 读取本地配置文件参数
    val properties = new Properties()
    properties.load(new FileInputStream(CommonConstant.confFile))

    val batch_interval = properties.getProperty("batch_interval", "60").toLong
    val broker_list = properties.getProperty("broker_list", "没有值")
    val topic_name = properties.getProperty("topic_name", "没有值")
    val group_id = properties.getProperty("group_id", "default_group_rowformat")
    val topic_name_in = properties.getProperty("topic_name_in", "没有值")
    val topic_name_out = properties.getProperty("topic_name_out", "没有值")
    val auto_offset_reset = properties.getProperty("auto.offset.reset", "latest")

    logInfo("------properties path:" + CommonConstant.confFile + "; batch_interval->" + batch_interval + "; broker_list->" + broker_list + "; topic_name-> " + topic_name + "; group_id-> " + group_id + "; topic_name_in-> " + topic_name_in + "; topic_name_out-> " + topic_name_out + "; auto_offset_reset-> " + auto_offset_reset + " ; ")

    logInfo("------Stream work dir: " + System.getProperty("user.dir"))
    logInfo("------1.初始化 streamingContext...------")

    val sparkConf = new SparkConf()
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
    sparkConf.setAppName("OCSP_KAFKA_ROW_FORMAT")
    val sc = new SparkContext(sparkConf)

    logInfo("------2.启动 streamingContext...------")
    val ssc = new StreamingContext(sc, Seconds(batch_interval))

    try {
      logInfo("------3.读取topic生成数据流DStream...------")
      val inputStream = readSource(ssc, broker_list, topic_name, group_id, auto_offset_reset)
      logInfo("3.1 读取topic生成DStream完毕...")

      inputStream.foreachRDD(rdd => {
        logInfo("------4.start inputStream流数据处理...------")
        val begin = System.currentTimeMillis()

        // ConsumerRecord格式转换为字符串
        val region_rdd = rdd.map(record => {
          record.value() //返回实际topic数据内容
        }).filter(line => line != null && line != "").persist()

        // Kafka send message
        region_rdd.foreachPartition(partition => {
          partition.foreach(inputStr => {
            if (inputStr != null) {
              // 避免“a,b,”这种split后最后一列丢失问题
              val inputArray = (inputStr + separator_column_field + "appendSplit").split(separator_column_field).dropRight(1)

              operateIn(inputArray, broker_list, topic_name_in)
              operateOut(inputArray, broker_list, topic_name_out)
            }
          })
        })

        region_rdd.unpersist()
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

  private def operateIn(inputArray: Array[String], broker_list: String, topic_name_in: String) = {

    logInfo("------4.1 流入区域按竖线拆分多行...")

    val IMSI = inputArray(0)
    val TIMESTAMP = inputArray(1)
    // 信令中的UTC时间戳
    val CITY_CODE = inputArray(10)
    // 基站归属地市
    val COUNTY_CODE = inputArray(11)
    // 基站归属区县
    val USR_PROV_CODE = inputArray(12)
    // 用户归属省份
    val USR_CITY_CODE = inputArray(13)
    // 用户归属地市
    val USR_COUNTY_CODE = inputArray(14)
    // 用户归属区县
    val SEX = inputArray(15)
    val AGE_LEVEL = inputArray(16)
    // 年龄等级
    val ARPU_LEVEL = inputArray(17)
    //消费等级
    val REGION_CODE = inputArray(9)
    // 当前区域编码（多个region以|分隔）
    val CURR_REGION_INTIME = inputArray(28)
    // 当前所在区域的流入时间（多个region时间以|分隔）
    val CURR_REGION_IN_OUT = inputArray(29)
    // 当前区域的流入流出标记（0=不变，1=流入，9=不详，多个标记以|分隔）
    val builder_in = new mutable.StringBuilder()
    val regionArray = REGION_CODE.split("\\|")
    val inTimeArray = CURR_REGION_INTIME.split("\\|")
    val regionFlagArray = CURR_REGION_IN_OUT.split("\\|")
    for (i <- 0 until regionArray.length) {
      builder_in.append(IMSI).append(separator_column_field)
      builder_in.append(regionArray(i)).append(separator_column_field)
      builder_in.append(TIMESTAMP).append(separator_column_field)
      builder_in.append(CITY_CODE).append(separator_column_field)
      builder_in.append(COUNTY_CODE).append(separator_column_field)
      builder_in.append(USR_PROV_CODE).append(separator_column_field)
      builder_in.append(USR_CITY_CODE).append(separator_column_field)
      builder_in.append(USR_COUNTY_CODE).append(separator_column_field)
      builder_in.append(SEX).append(separator_column_field)
      builder_in.append(AGE_LEVEL).append(separator_column_field)
      builder_in.append(ARPU_LEVEL).append(separator_column_field)
      builder_in.append(inTimeArray(i)).append(separator_column_field)
      builder_in.append(regionFlagArray(i))
      builder_in.append(separator_row_field)
    }

    val messages = (builder_in.toString() + "appendSplit").split(separator_row_field).dropRight(1)
    for (message: String <- messages) {
      KafkaSendTool.sendMessage(new ProducerRecord[String, String](topic_name_in, IMSI, message), broker_list)
    }

  }

  private def operateOut(inputArray: Array[String], broker_list: String, topic_name_out: String) = {

    logInfo("------4.2 流出区域按竖线拆分多行...")

    val IMSI = inputArray(0)
    val TIMESTAMP = inputArray(1) // 信令中的UTC时间戳
    val OUT_REGION = inputArray(30) // 流出区域编码（多个区域以|分隔）
    val OUT_REGION_IN_DT = inputArray(31) // 流出区域的流入时间（多个流入时间以|分隔）
    val OUT_REGION_OUT_DT = inputArray(32) // 流出区域的流出时间（多个流出时间以|分隔）
    val OUT_REGION_STAY = inputArray(33) // 流出区域的驻留时长（多个驻留以|分隔）
    val OUT_CITY_CODE = inputArray(24) // 流出基站归属地市
    val OUT_COUNTY_CODE	= inputArray(25) // 流出基站归属区县

    val builder_out = new mutable.StringBuilder()
    val outRegionArray = OUT_REGION.split("\\|")
    val inTimeArray = OUT_REGION_IN_DT.split("\\|")
    val outTimeArray = OUT_REGION_OUT_DT.split("\\|")
    val stayArray = OUT_REGION_STAY.split("\\|")
    for (i <- 0 until outRegionArray.length) {
      builder_out.append(IMSI).append(separator_column_field)
      builder_out.append(outRegionArray(i)).append(separator_column_field)
      builder_out.append(TIMESTAMP).append(separator_column_field)
      builder_out.append(inTimeArray(i)).append(separator_column_field)
      builder_out.append(outTimeArray(i)).append(separator_column_field)
      builder_out.append(stayArray(i)).append(separator_column_field)
      builder_out.append(OUT_CITY_CODE).append(separator_column_field)
      builder_out.append(OUT_COUNTY_CODE)
      builder_out.append(separator_row_field)
    }

    val messages = (builder_out.toString() + "appendSplit").split(separator_row_field).dropRight(1)
    for (message: String <- messages) {
      KafkaSendTool.sendMessage(new ProducerRecord[String, String](topic_name_out, IMSI, message), broker_list)
    }

  }

  /**
    * 读取kafka源数据
    *
    * @param ssc
    * @return
    */
  def readSource(ssc: StreamingContext, broker_list: String, topic_name: String, group_id: String, offset_reset: String): DStream[ConsumerRecord[String, String]] = {

    //根据输入数据接口配置，生成数据流 DStream
    val dataSource = new KafkaReader(ssc, kafka_kerberos_enable, kafka_location_preferbroker, broker_list, topic_name, group_id, offset_reset)
    dataSource.createStreamMulData()
  }

}
