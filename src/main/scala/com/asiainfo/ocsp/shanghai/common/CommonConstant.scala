package com.asiainfo.ocsp.shanghai.common

import java.io.{File, FileInputStream}
import java.util.Properties

object CommonConstant {

  //获取当前jar包的绝对路径：/主程序路径/lib，执行spark-submit命令所在路径bin下必须有kafka的keytab文件
  val jarFilePath = CommonConstant.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
  val baseDir_decode = java.net.URLDecoder.decode(jarFilePath, "UTF-8");
  val baseDir = (new File(baseDir_decode)).getParent

  // common.properties文件放到spark-submit的jvm参数中上传到每个container中，必须是当前相对路径
  val confFile = new File(baseDir, "../conf/common.properties").getCanonicalPath

}
