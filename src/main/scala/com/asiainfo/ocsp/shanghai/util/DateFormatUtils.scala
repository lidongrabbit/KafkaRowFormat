package com.asiainfo.ocsp.shanghai.util

/**
  * Created by yang on 2017/12/15.
  */
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by tsingfu on 15/8/18.
  */
object DateFormatUtils {

  val DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss"
  val TIME_PATTERN = "HH:mm:ss"

  val defaultSDF = new SimpleDateFormat(DEFAULT_PATTERN)


  val threadLocalSDF = new ThreadLocal[SimpleDateFormat]{
    override def initialValue() = new SimpleDateFormat()
  }

  //毫秒转换为字符串
  def dateMs2Str(dateMs: Long): String ={
    defaultSDF.format(new Date(dateMs))
  }

  //字符串转换为毫秒
  def dateStr2Ms(dateStr: String): Long ={
    defaultSDF.parse(dateStr).getTime
  }


  def dateMs2Str(dateMs: Long, pattern: String): String ={
    threadLocalSDF.get().applyPattern(pattern)
    threadLocalSDF.get().format(new Date(dateMs))
  }

  def dateStr2Ms(dateStr: String, pattern: String): Long ={
    threadLocalSDF.get().applyPattern(pattern)
    threadLocalSDF.get().parse(dateStr).getTime
  }
}
