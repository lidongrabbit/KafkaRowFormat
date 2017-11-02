package com.asiainfo.ocsp

/**
  * Created by lidong on 2017/10/31.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val topic="test"
    val topicSet = topic.split(",").toSet
    println(topicSet.size)
  }
}
