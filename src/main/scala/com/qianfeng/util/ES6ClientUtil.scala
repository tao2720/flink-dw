package com.qianfeng.util

import java.net.InetSocketAddress
import java.util

import com.qianfeng.constant.Constants
import com.qianfeng.util.ESConfigUtil.ESConfigSocket
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * es的客户端 transport
 */
object ES6ClientUtil {
  //获取打印日志对象
  private val logger: Logger = LoggerFactory.getLogger(ES6ClientUtil.getClass)

  /**
   * 获取es客户端
   * @param esConfigPath
   * @return
   */
  def buildTransportClient(esConfigPath:String = Constants.ES_CONFIG_URL):PreBuiltTransportClient = {
    //判断esconfigpath是否为空
    if(esConfigPath == null){
      throw new RuntimeException("esconfigpath is null")
    }

    //初始化一个客户端
    var transportClient: PreBuiltTransportClient = null
    try {
      //获取es的config
      val esConfig: ESConfigSocket = ESConfigUtil.getConfigSocket(esConfigPath)
      //获取esconfig的地址集合
      val transAddrs: mutable.Buffer[InetSocketAddress] = esConfig.transportAddresses.asScala

      //获取es的settings对象
      val settings: Settings.Builder = Settings.builder()
      //设置集群名称信息
      for ((key,value) <- esConfig.config.asScala) {
        settings.put(key,value)
      }

      //设置主机名和端口
      transportClient = new PreBuiltTransportClient(settings.build())
      //设置主机名和端口
      for(transAddr <- transAddrs){
        val address: TransportAddress = new TransportAddress(transAddr)
        //将TransportAddress地址循环添加到transportClient对象中
        transportClient.addTransportAddress(address)
      }
    } catch {
      case e:Exception => e.printStackTrace()
        logger.error("get transportClient error......")
    }
    //返回客户端
    transportClient
  }


  //测试
  def main(args: Array[String]): Unit = {
    val transportClient: PreBuiltTransportClient = buildTransportClient()
    //println(transportClient)

    //测试是否能更新索引
    val indexName = "user"
    val esID = "110"
    var value = new util.HashMap[String,String]()
    value.put("name","mazi")
    value.put("age","18")
    //获取indexrequest
    val indexRequest: IndexRequest = new IndexRequest(indexName, "book", esID)
      .source(value)
    val response: UpdateResponse = transportClient.prepareUpdate(indexName, "book", esID)
      .setDoc(value)
      .setUpsert(indexRequest)
      .get()
    println(response.status().getStatus)
  }
}
