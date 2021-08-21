package com.qianfeng.util

import java.util.Properties

import com.qianfeng.constant.Constants

/**
 * properties的工具类
 */
object PropertiesUtil {

    //读取指定路径下的properties文件
  def readProperty(path:String): Properties ={
    //使用类加载器加载
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(path)
    //定义一个pro对象
    val prop = new Properties()
    prop.load(inputStream)
    prop  //返回
  }

  //测试
  def main(args: Array[String]): Unit = {
    print(readProperty(Constants.KAFKA_CONSUMER_CONFIG_URL).get("bootstrap.servers"))
  }

}
