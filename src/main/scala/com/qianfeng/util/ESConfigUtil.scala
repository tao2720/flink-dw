package com.qianfeng.util

import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import com.qianfeng.constant.Constants
import org.apache.flink.shaded.hadoop2.org.apache.http.HttpHost
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}


/**
 * es的config配置文件加载，，返回config对象
 */
object ESConfigUtil {

  //定义两个变量
  var esConfigSocket: ESConfigSocket = null
  var esConfigHttpHost : ESConfigHttpHost = null

  //定义封装socket的配置类
  class ESConfigSocket(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[InetSocketAddress])
  //定义封装http的配置类
  class ESConfigHttpHost(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[HttpHost])


  /**
   * 客户端连接
   * @param configPath
   * @return
   */
  def getConfigSocket(configPath: String): ESConfigSocket = {
    //使用类加载器加载配置文件
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)

    //判断esConfigSocket是否为空
    if (null == esConfigSocket) {
      //使用databind包中的ObjectMapper
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)
      //读取jsonnode中config的节点
      val configJsonNode = configJsonObject.get("config")
      //把configJsonNode读取出来，存储到map中
      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()  //cluster.name  和 client.transport.sniff
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      //构建List
      val addressJsonNode = configJsonObject.get("address")
      //addressJsonNode转换成ArrayNode
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses:java.util.ArrayList[InetSocketAddress] = {
        //初始化一个集合
        val transportAddresses = new java.util.ArrayList[InetSocketAddress]
        //addressJsonArray转换成迭代器
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          //先将每一个的ip和port封装InetSocketAddress对象中，然后再将InetSocketAddress对象添加到List中
          transportAddresses.add(new InetSocketAddress(InetAddress.getByName(ip), port))
        }
        transportAddresses
      }
      //封装ESConfigSocket并赋值给esConfigSocket
      esConfigSocket = new ESConfigSocket(config, transportAddresses)
    }
    esConfigSocket
  }


  /**
   *
   * @param configPath
   * @return
   */
  def getConfigHttpHost(configPath: String): ESConfigHttpHost = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == esConfigHttpHost) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")

      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      val addressJsonNode = configJsonObject.get("address")
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses:java.util.ArrayList[HttpHost] = {
        val httpHosts = new java.util.ArrayList[HttpHost]
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          val schema = "http"
          //封装httpHost对象
          val httpHost = new HttpHost(ip, port, schema)
          httpHosts.add(httpHost)
        }
        httpHosts
      }
      //使用config, transportAddresses封装ESConfigHttpHost对象，赋值esConfigHttpHost
      esConfigHttpHost = new ESConfigHttpHost(config, transportAddresses)
    }
    esConfigHttpHost
  }


  //测试
  def main(args: Array[String]): Unit = {
    println(getConfigSocket(Constants.ES_CONFIG_URL).config.get("cluster.name"))
    println(getConfigSocket(Constants.ES_CONFIG_URL).transportAddresses.get(0).getPort)

    println(getConfigHttpHost(Constants.ES_CONFIG_URL).transportAddresses.get(0).getHostName)
  }
}
