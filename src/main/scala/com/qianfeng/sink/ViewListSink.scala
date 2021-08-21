package com.qianfeng.sink

import com.qianfeng.JsonUtil
import com.qianfeng.constant.Constants
import com.qianfeng.rdo.LogData.DWDViewListDetail
import com.qianfeng.util.ES6ClientUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Response
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}


/**
 * 用户产品列表浏览sink
 *
 * @param indexName
 */
class ViewListSink(indexName:String) extends RichSinkFunction[DWDViewListDetail]{

  //日志记录
  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  //ES客户端连接对象
  var transportClient: PreBuiltTransportClient = _

  /**
   * 连接es集群
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    //flink与es网络通信参数设置(默认虚核)
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    transportClient = ES6ClientUtil.buildTransportClient()
    super.open(parameters)
  }

  /**
   * Sink输出处理
   * @param value
   * @param context
   */
  override def invoke(value: DWDViewListDetail, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val result :java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
      //数据检测
      val checkResult: String = checkData(result)
      //判断检测结果是否不为空  ，，不等于则result|action|eventType|ct中任意有一个为空，不允许存储。反之存储
      if (StringUtils.isNotBlank(checkResult)) {
        //日志记录
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }
      //请求id
      val sid = value.sid+"_"+value.targetID
      //索引名称、类型名称 --- 插入es中
      handleData(indexName, indexName, sid, result)

    }catch{
      case ex: Exception => logger.error(ex.getMessage)
    }
  }

  /**
   * ES插入或更新数据
   * @param idxName
   * @param idxTypeName
   * @param esID
   * @param value
   */
  def handleData(idxName :String, idxTypeName :String, esID :String,
                 value: java.util.Map[String,Object]): Unit ={
    //构建IndexRequest请求对象
    val indexRequest = new IndexRequest(idxName, idxName, esID).source(value)

    //使用客户端更新数据
    val response: UpdateResponse = transportClient.prepareUpdate(idxName, idxName, esID)
      .setRetryOnConflict(Constants.ES_RETRY_NUMBER)
      .setDoc(value)  //设置整行的值
      .setUpsert(indexRequest)
      .get()

    //判断插入结果
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run exception:status:" + response.status().name())
    }
  }

  /**
   * 资源关闭
   */
  override def close() = {
    if (this.transportClient != null) {
      this.transportClient.close()
    }
  }

  /**
   * 参数校验
   * @param value
   * @return
   */
  def checkData(value :java.util.Map[String,Object]): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //行为类型
    val action = value.get(Constants.LOG_ACTION)
    if(null == action){
      msg = "Travel.ESSink.action  is null"
    }

    //事件类型
    val eventType = value.get(Constants.LOG_EVENT_TYPE)
    if(null == eventType){
      msg = "Travel.ESSink.eventtype  is null"
    }

    //时间
    val ctNode = value.get(Constants.LOG_CT)
    if(null == ctNode){
      msg = "Travel.ESSink.ct is null"
    }
    msg
  }
}
