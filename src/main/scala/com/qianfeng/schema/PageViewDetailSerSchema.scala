package com.qianfeng.schema

import java.lang

import com.qianfeng.JsonUtil
import com.qianfeng.rdo.LogData.{DWDClickDetail, DWDPageViewDetail}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

//用户浏览明细序列化
class PageViewDetailSerSchema(toTopic:String) extends KafkaSerializationSchema[DWDPageViewDetail]{
  override def serialize(element: DWDPageViewDetail, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //构造key
    val key = element.userID+"_"+element.sid
    val value:String = JsonUtil.gObject2Json(element)
    //构造返回
    new ProducerRecord[Array[Byte], Array[Byte]](toTopic,key.getBytes,value.getBytes)
  }
}
