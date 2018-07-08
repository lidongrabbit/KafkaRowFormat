package com.asiainfo.ocsp.shanghai.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable

/**
  * Created by tsingfu on 15/8/18.
  */
object KafkaSendTool {

  val DEFAULT_SERIALIZER_CLASS = "kafka.serializer.StringEncoder"

  // surq:一个datasource 对应一个producer，跟业务个数无直接关系
  val dsid2ProducerMap = mutable.Map[String, KafkaProducer[String, String]]()

  // 多线程、多producer,分包发送
  def sendMessage(message: List[ProducerRecord[String, String]], broker_list: String) = {
    val msgList: Iterator[List[ProducerRecord[String, String]]] = message.sliding(200, 200)
    if (msgList.size > 0) {
      val producer: KafkaProducer[String, String] = getProducer(broker_list)
      message.sliding(200, 200).foreach((list: List[ProducerRecord[String, String]]) => {
        list.foreach((record: ProducerRecord[String, String]) => {
          producer.send(record)
        })
      })
    }
  }

  // 一条条发送
  def sendMessage(message: ProducerRecord[String, String], broker_list: String) = {
    val producer: KafkaProducer[String, String] = getProducer(broker_list)
    producer.send(message)
  }

  // 对应的producer若不存在，则创建新的producer，并存入dsid2ProducerMap
  private def getProducer(broker_list: String): KafkaProducer[String, String] =
    dsid2ProducerMap.getOrElseUpdate("datasource1", {
      val props = new Properties()
      props.put("bootstrap.servers", broker_list)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("security.protocol", "SASL_PLAINTEXT")
      //props.put("serializer.class", dsConf.get("serializer.class", DEFAULT_SERIALIZER_CLASS))
      new KafkaProducer[String, String](props)
    })


}