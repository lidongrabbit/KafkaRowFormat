package com.asiainfo.ocsp.shanghai.common

import java.io.FileInputStream
import java.util.Properties

import scala.beans.BeanProperty

/**
  * Created by rainday on 12/10/17.
  */
class AppConf extends Serializable with Logging {

  val properties = new Properties()
  properties.load(new FileInputStream(CommonConstant.confFile))

  // 基本配置
  @BeanProperty val batch_interval = properties.getProperty("batch_interval", "60").toLong
  @BeanProperty val key_index = properties.getProperty("input.field.key.index", "").split(",")
  @BeanProperty val time_index = properties.getProperty("input.field.time.index", "").toInt
  @BeanProperty val distinct_time_enable = properties.getProperty("distinct.time.enable", "true").toBoolean

  // kafka配置
  @BeanProperty val kafka_offset_type = properties.getProperty("auto.offset.reset", "latest")
  @BeanProperty val kafka_kerberos_enable = properties.getProperty("kafka.kerberos.enable", "false").toBoolean
  @BeanProperty val kafka_location_preferbroker = properties.getProperty("kafka.location_preferbroker", "false").toBoolean
  @BeanProperty val broker_list = properties.getProperty("broker_list", "")
  @BeanProperty val topic_name_in = properties.getProperty("topic_name_in", "")
  @BeanProperty val topic_name_out = properties.getProperty("topic_name_out", "")
  @BeanProperty val group_id = properties.getProperty("group_id", "lte_distinct")
  @BeanProperty val default_separator = properties.getProperty("default_separator", ",")

  logInfo("kafka params:------------------------------------- ")
  logInfo("stream batch interval: " + batch_interval)
  logInfo("input.field.key.index: " + key_index)
  logInfo("input.field.time.index: " + time_index)
  logInfo("distinct.time.enable: " + distinct_time_enable)

  logInfo("kafka.broker_list: " + broker_list)
  logInfo("kafka.topic.in : " + topic_name_in)
  logInfo("kafka.topic.out : " + topic_name_out)
  logInfo("kafka.group_id: " + group_id)
  logInfo("kafka_offset_type: " + kafka_offset_type)
  logInfo("kafka.kerberos.enable : " + kafka_kerberos_enable)
  logInfo("kafka.location_preferbroker: " + kafka_location_preferbroker)
  logInfo("default_separator: " + default_separator)

}



